// PREVIEW: no released Dostoevsky server speaks the wire protocol yet.
import { ClientError, type Receipt, type StreamRecord, type Position } from "./index.js";

export { ClientError };
export class ConnectionLost extends ClientError { readonly sent: boolean }
export type Target = { host?: string; port?: number } | { path: string };
export interface ConnectOptions { tls?: boolean | { ca?: string | Uint8Array; servername?: string } }
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
