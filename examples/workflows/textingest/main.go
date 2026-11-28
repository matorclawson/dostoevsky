// Text ingestion: on an upload event, download a document, split and embed it,
// and write the vectors. Steps pass references, because a record is small.
//
// Mirrors the text path of github.com/restatedev/examples (MIT),
// python/end-to-end-applications/rag-ingestion. The object store, embedding
// model and vector store are local stand-ins. The PDF path, with one call per
// chunk, needs fan-out and join, which the module does not have.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"time"

	"github.com/rodriveracom/dostoevsky/workflow"
)

const name = "ingest-text"
const queue = "ingest"
const chunkRunes = 1000
const overlapRunes = 200

type chunk struct {
	Text   string `json:"text"`
	Vector uint32 `json:"vector"`
}

type reference struct {
	Object string `json:"object"`
	Key    string `json:"key"`
}

// services stands for the outside systems. Put and Upsert replace by key, so
// repeating them is harmless.
type services interface {
	Get(key string) ([]byte, error)
	Put(key string, value []byte) error
	Upsert(id string, c chunk) error
}

func register(s *workflow.Store) error {
	var steps []workflow.Step
	for _, step := range []string{"download", "embed", "upsert"} {
		steps = append(steps, workflow.Step{Kind: "activity", Name: step, Attempts: 20})
	}
	return s.Register(name, 1, steps)
}

// onUpload starts one run per upload event.
func onUpload(s *workflow.Store, eventID, object string) error {
	input, err := json.Marshal(reference{Object: object, Key: object})
	if err != nil {
		return err
	}
	_, err = s.StartOnce(name+"/"+eventID, name, 1, queue, input, nil)
	return err
}

func split(text string) []string {
	runes := []rune(text)
	var parts []string
	for from := 0; from < len(runes); from += chunkRunes - overlapRunes {
		to := min(from+chunkRunes, len(runes))
		parts = append(parts, string(runes[from:to]))
		if to == len(runes) {
			break
		}
	}
	return parts
}

// embed is a stand-in for an embedding model.
func embed(text string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(text))
	return h.Sum32()
}

func perform(task *workflow.Task, ref reference, out services) (reference, error) {
	data, err := out.Get(ref.Key)
	if err != nil {
		return ref, err
	}
	switch task.Activity {
	case "download":
		ref.Key = task.Run + "/text"
		return ref, out.Put(ref.Key, data)
	case "embed":
		var chunks []chunk
		for _, text := range split(string(data)) {
			chunks = append(chunks, chunk{Text: text, Vector: embed(text)})
		}
		encoded, err := json.Marshal(chunks)
		if err != nil {
			return ref, err
		}
		ref.Key = task.Run + "/vectors"
		return ref, out.Put(ref.Key, encoded)
	case "upsert":
		var chunks []chunk
		if err = json.Unmarshal(data, &chunks); err != nil {
			return ref, err
		}
		for i, c := range chunks {
			if err = out.Upsert(fmt.Sprintf("%s#%d", ref.Object, i), c); err != nil {
				return ref, err
			}
		}
		return ref, nil
	}
	return ref, errors.New("unknown activity")
}

func handle(task *workflow.Task, out services) ([]byte, string) {
	var ref reference
	if err := json.Unmarshal(task.Input, &ref); err != nil {
		return nil, err.Error()
	}
	next, err := perform(task, ref, out)
	if err != nil {
		return nil, err.Error()
	}
	encoded, err := json.Marshal(next)
	if err != nil {
		return nil, err.Error()
	}
	return encoded, ""
}

type memory struct {
	objects map[string][]byte
	vectors map[string]chunk
}

func (m memory) Get(key string) ([]byte, error) {
	value, ok := m.objects[key]
	if !ok {
		return nil, fmt.Errorf("no object %q", key)
	}
	return value, nil
}

func (m memory) Put(key string, value []byte) error {
	m.objects[key] = value
	return nil
}

func (m memory) Upsert(id string, c chunk) error {
	m.vectors[id] = c
	return nil
}

func main() {
	if len(os.Args) != 3 {
		log.Fatal("usage: textingest DATA-DIRECTORY TEXT-FILE")
	}
	text, err := os.ReadFile(os.Args[2])
	if err != nil {
		log.Fatal(err)
	}
	// The stand-in services live in memory, so use a new data directory each time.
	world := memory{map[string][]byte{"upload": text}, map[string]chunk{}}
	s, err := workflow.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	if err = register(s); err != nil {
		log.Fatal(err)
	}
	if err = onUpload(s, "event-1", "upload"); err != nil {
		log.Fatal(err)
	}
	for {
		task, err := s.Poll(queue, time.Minute)
		if err != nil {
			log.Fatal(err)
		}
		if task == nil {
			break
		}
		out, failure := handle(task, world)
		if err = s.Finish(task.Run, task.Token, out, failure); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("vectors:", len(world.vectors))
}
