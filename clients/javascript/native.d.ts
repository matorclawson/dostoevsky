// PREVIEW: no released Dostoevsky server speaks the wire protocol yet.
import { ClientError, type Receipt, type StreamRecord, type Position } from "./index.js";

export { ClientError };
export class ConnectionLost extends ClientError { readonly sent: boolean }
/** The client's own queue is full: refused here, never sent. */
export class Overloaded extends ClientError {}
export type Target = { host?: string; port?: number } | { path: string };
export interface ConnectOptions {
  /** Not allowed with a unix socket. */
  tls?: boolean | { ca?: string | Uint8Array; servername?: string };
  /** Unanswered requests this client will hold, queued ones included (default 1024). */
  maxRequests?: number;
  /** Their frame bytes (default 8 MiB, at least one maximum frame). */
  maxBytes?: number;
}
export class NativeClient {
  private constructor();
  static connect(target: Target, token: string, options?: ConnectOptions): Promise<NativeClient>;
  publish(topic: string, value: Uint8Array, options?: {key?: string; id?: string}): Promise<Receipt>;
  read(topic: string, partition: number, offset: bigint, waitMs?: number): Promise<StreamRecord | null>;
  position(topic: string, partition: number, group: string): Promise<Position>;
  commit(topic: string, partition: number, group: string, expected: bigint, state?: Uint8Array): Promise<void>;
  ping(): Promise<void>;
  close(): void;
}
