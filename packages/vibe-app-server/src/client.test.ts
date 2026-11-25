import { describe, expect, it } from "@effect/vitest";

import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";

import * as Result from "effect/Result";
import * as Schema from "effect/Schema";

import * as VibeClient from "./client.ts";
import * as VibeError from "./errors.ts";
import * as VibeProtocol from "./protocol.ts";

const isRequestError = Schema.is(VibeError.VibeAppServerRequestError);

describe("parseJsonRpcLine", () => {
  it("parses requests, notifications, successes and failures", () => {
    const parse = VibeProtocol.parseJsonRpcLine;
    expect(
      VibeProtocol.isJsonRpcRequest(parse('{"jsonrpc":"2.0","id":1,"method":"session/start"}')!),
    ).toBe(true);
    expect(
      VibeProtocol.isJsonRpcNotification(parse('{"jsonrpc":"2.0","method":"turn/started"}')!),
    ).toBe(true);
    expect(
      VibeProtocol.isJsonRpcSuccess(parse('{"jsonrpc":"2.0","id":1,"result":{"ok":true}}')!),
    ).toBe(true);
    expect(
      VibeProtocol.isJsonRpcFailure(
        parse('{"jsonrpc":"2.0","id":1,"error":{"code":"conflict","message":"x"}}')!,
      ),
    ).toBe(true);
    expect(parse("not json")).toBeUndefined();
    expect(parse("")).toBeUndefined();
  });
});

/* Let queued fibers run until the in-memory pipes are drained. */
const flushQueues = () =>
  Effect.gen(function* () {
    for (let i = 0; i < 10; i++) yield* Effect.yieldNow;
  });

const encoder = new TextEncoder();
const serverLine = (input: Queue.Queue<Uint8Array>, payload: unknown) =>
  Queue.offer(input, encoder.encode(`${JSON.stringify(payload)}\n`));

/** Fake vibe-app-server: answers initialize/account/read, fails session/stop, notifies on anything else. */
const seenRequests: Array<{ method: string; params: unknown }> = [];
let fakeGoal: VibeClient.PublicSessionGoal | null = null;

const fakeServer = (
  output: Queue.Queue<string>,
  input: Queue.Queue<Uint8Array>,
  options: { readonly goalMethods?: "supported" | "missing" } = {},
): Effect.Effect<never> =>
  Effect.forever(
    Effect.gen(function* () {
      const line = yield* Queue.take(output);
      const message = VibeProtocol.parseJsonRpcLine(line);
      if (!message || !VibeProtocol.isJsonRpcRequest(message)) return;
      seenRequests.push({ method: message.method, params: message.params });
      switch (message.method) {
        case "initialize":
          return yield* serverLine(input, {
            jsonrpc: "2.0",
            id: message.id,
            result: { serverInfo: { name: "vibe-app-server", version: "9.9.9" }, processId: 42 },
          });
        case "account/read":
          return yield* serverLine(input, {
            jsonrpc: "2.0",
            id: message.id,
            result: {
              account: {
                status: "ready",
                plan: { kind: "chat", name: "PRO", title: "Pro" },
                budget: {
                  usagePercentage: 12.5,
                  resetAt: "2026-12-01T00:00:00Z",
                  paygEnabled: false,
                },
              },
            },
          });
        case "session/stop":
          return yield* serverLine(input, {
            jsonrpc: "2.0",
            id: message.id,
            error: { code: "conflict", message: "no session" },
          });
        case "session/list":
          return yield* serverLine(input, {
            jsonrpc: "2.0",
            id: message.id,
            result: {
              items: [
                {
                  id: "s1",
                  tokenUsage: { inputTokens: 120, cachedInputTokens: 90 },
                  goal: {
                    goalId: "goal-1",
                    revision: 2,
                    objectiveRevision: 1,
                    objective: "Ship the feature",
                    status: "paused",
                    statusReason: "user",
                    tokenBudget: null,
                    tokensUsed: 40,
                    timeUsedSeconds: 3,
                    createdAt: 1782622440000,
                    updatedAt: 1782622450000,
                  },
                },
              ],
            },
          });
        case "goal/get":
          if (options.goalMethods === "missing") {
            return yield* serverLine(input, {
              jsonrpc: "2.0",
              id: message.id,
              error: { code: "method_not_found", message: "goals are not available" },
            });
          }
          return yield* serverLine(input, {
            jsonrpc: "2.0",
            id: message.id,
            result: { goal: fakeGoal },
          });
        case "goal/set": {
          const params = (message.params ?? {}) as Record<string, unknown>;
          const previous = fakeGoal ?? {
            goalId: "goal-1",
            revision: 0,
            objectiveRevision: 0,
            objective: "",
            status: "paused" as VibeClient.PublicSessionGoal["status"],
            statusReason: null,
            tokenBudget: null as number | null,
            tokensUsed: 0,
            timeUsedSeconds: 0,
            createdAt: 1782622440000,
            updatedAt: 1782622450000,
          };
          const objective =
            typeof params["objective"] === "string" ? params["objective"] : previous.objective;
          const status =
            params["status"] === "active" || params["status"] === "paused"
              ? params["status"]
              : previous.status;
          const tokenBudget =
            params["tokenBudget"] === undefined
              ? previous.tokenBudget
              : (params["tokenBudget"] as number | null);
          fakeGoal = {
            ...previous,
            revision: previous.revision + 1,
            objectiveRevision:
              objective !== previous.objective
                ? previous.objectiveRevision + 1
                : previous.objectiveRevision,
            objective,
            status,
            tokenBudget,
            updatedAt: 1782622450000,
          };
          return yield* serverLine(input, {
            jsonrpc: "2.0",
            id: message.id,
            result: { goal: fakeGoal },
          });
        }
        case "goal/clear": {
          const cleared = fakeGoal !== null;
          fakeGoal = null;
          return yield* serverLine(input, {
            jsonrpc: "2.0",
            id: message.id,
            result: { cleared },
          });
        }
        default:
          yield* serverLine(input, {
            jsonrpc: "2.0",
            method: "turn/started",
            params: { eventId: 1, sessionId: "s1" },
          });
          return yield* serverLine(input, { jsonrpc: "2.0", id: message.id, result: {} });
      }
    }),
  );

describe("VibeAppServerClient over in-memory stdio", () => {
  it.effect("round-trips typed requests, dispatches notifications, maps errors", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { stdio, input, output } = yield* VibeClient.makeInMemoryStdio();
        const serverFiber = yield* Effect.forkScoped(fakeServer(output, input));
        const client = yield* VibeClient.make(stdio);

        const notifications: Array<{ method: string; params: unknown }> = [];
        yield* client.handleUnknownServerNotification((method, params) =>
          Effect.sync(() => {
            notifications.push({ method, params });
          }),
        );

        const init = yield* client.initialize({ clientName: "test", clientVersion: "0.0.0" });
        expect(init.serverInfo.version).toBe("9.9.9");
        yield* client.initialized();

        const account = yield* client.accountRead("s1");
        expect(account.status).toBe("ready");
        expect(account.budget?.usagePercentage).toBe(12.5);
        expect((yield* client.sessionList()).items[0]?.tokenUsage?.cachedInputTokens).toBe(90);

        // JSON-RPC error surfaces as a typed request error with method attached.
        const outcome = yield* Effect.result(client.sessionStop({ sessionId: "s1" }));
        const requestError =
          Result.isFailure(outcome) && isRequestError(outcome.failure)
            ? outcome.failure
            : undefined;
        expect(requestError?._tag).toBe("VibeAppServerRequestError");
        expect(requestError?.method).toBe("session/stop");
        expect(requestError?.code).toBe("conflict");

        // Unknown method triggers the notification path on the fake server.
        yield* client.sessionDelete("s1");
        yield* flushQueues();
        expect(notifications.some((n) => n.method === "turn/started")).toBe(true);

        // Pagination goes under `page`, and callback answers nest `{ callbackId, output }` in `result`.
        yield* client
          .sessionHistoryList({ sessionId: "s1", limit: 5, cursor: "c1" })
          .pipe(Effect.ignore);
        yield* client.sessionTurnsList({ sessionId: "s1", limit: 5 }).pipe(Effect.ignore);
        yield* client
          .callbackResult({ sessionId: "s1", callbackId: "cb1", result: { outcome: "approved" } })
          .pipe(Effect.ignore);
        yield* flushQueues();
        const historyReq = seenRequests.find((r) => r.method === "session/history/list");
        expect(historyReq?.params).toEqual({ sessionId: "s1", page: { limit: 5, cursor: "c1" } });
        const turnsReq = seenRequests.find((r) => r.method === "session/turns/list");
        expect(turnsReq?.params).toEqual({ sessionId: "s1", page: { limit: 5 } });
        const callbackReq = seenRequests.find((r) => r.method === "callback/result");
        expect(callbackReq?.params).toEqual({
          sessionId: "s1",
          result: { callbackId: "cb1", output: { outcome: "approved" } },
        });

        yield* Fiber.interrupt(serverFiber);
      }),
    ),
  );
});

describe("VibeAppServerClient goal methods", () => {
  it.effect("round-trips the goal lifecycle with compare-and-swap params", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { stdio, input, output } = yield* VibeClient.makeInMemoryStdio();
        const serverFiber = yield* Effect.forkScoped(fakeServer(output, input));
        const client = yield* VibeClient.make(stdio);
        yield* client.initialize({ clientName: "test", clientVersion: "0.0.0" });
        yield* client.initialized();
        fakeGoal = null;

        expect(yield* client.goalGet("s1")).toBe(null);

        const created = yield* client.goalSet({
          sessionId: "s1",
          objective: "Ship the feature",
          status: "paused",
        });
        expect(created.status).toBe("paused");
        expect(created.revision).toBe(1);

        const resumed = yield* client.goalSet({
          sessionId: "s1",
          expectedGoalId: created.goalId,
          status: "active",
          tokenBudget: null,
        });
        expect(resumed.status).toBe("active");
        expect(resumed.tokenBudget).toBe(null);

        expect(yield* client.goalGet("s1")).toEqual(resumed);
        expect(yield* client.goalClear({ sessionId: "s1", expectedGoalId: resumed.goalId })).toBe(
          true,
        );
        expect(yield* client.goalGet("s1")).toBe(null);

        yield* flushQueues();
        const setReq = seenRequests.find((r) => r.method === "goal/set");
        expect(setReq?.params).toEqual({
          sessionId: "s1",
          objective: "Ship the feature",
          status: "paused",
        });
        const clearReq = seenRequests.find((r) => r.method === "goal/clear");
        expect(clearReq?.params).toEqual({ sessionId: "s1", expectedGoalId: resumed.goalId });

        yield* Fiber.interrupt(serverFiber);
      }),
    ),
  );

  it.effect("decodes a session goal and surfaces method_not_found from servers predating goals", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { stdio, input, output } = yield* VibeClient.makeInMemoryStdio();
        const serverFiber = yield* Effect.forkScoped(
          fakeServer(output, input, { goalMethods: "missing" }),
        );
        const client = yield* VibeClient.make(stdio);
        yield* client.initialize({ clientName: "test", clientVersion: "0.0.0" });
        yield* client.initialized();

        const session = (yield* client.sessionList()).items[0];
        expect(session?.goal?.objective).toBe("Ship the feature");
        expect(session?.goal?.status).toBe("paused");

        const outcome = yield* Effect.result(client.goalGet("s1"));
        const requestError =
          Result.isFailure(outcome) && isRequestError(outcome.failure)
            ? outcome.failure
            : undefined;
        expect(requestError?._tag).toBe("VibeAppServerRequestError");
        expect(requestError?.method).toBe("goal/get");
        expect(requestError?.code).toBe("method_not_found");

        yield* Fiber.interrupt(serverFiber);
      }),
    ),
  );
});
