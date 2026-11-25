import { describe, expect, it } from "@effect/vitest";

import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as VibeClient from "./client.ts";
import * as VibeError from "./errors.ts";

const isProtocolError = Schema.is(VibeError.VibeAppServerProtocolError);

describe("vibe-app-server live smoke (skips when the binary is absent)", () => {
  it.effect("initialize, ephemeral session in cwd, config + git checkouts read, delete", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const child = yield* spawner
          .spawn(ChildProcess.make("vibe-app-server", [], { cwd: process.cwd() }))
          .pipe(
            Effect.catchCause((cause) =>
              Effect.fail(
                new VibeError.VibeAppServerProtocolError({
                  detail: `skip: vibe-app-server is not installed (${String(Cause.squash(cause))})`,
                  cause,
                }),
              ),
            ),
          );
        const client = Context.get(
          yield* Layer.build(VibeClient.layerChildProcess(child)),
          VibeClient.VibeAppServerClient,
        );

        const init = yield* client.initialize({ clientName: "t3-test", clientVersion: "0.0.0" });
        expect(init.serverInfo.name).toBe("vibe-app-server");
        expect(init.serverInfo.version.length).toBeGreaterThan(0);
        yield* client.initialized();

        const started = yield* client.sessionStart({
          kind: "ephemeral",
          agentConfig: { cwd: process.cwd(), headless: true, trustWorkspace: false },
        });
        const sessionId = started.state.session.id;
        expect(sessionId.length).toBeGreaterThan(0);
        expect(started.state.session.cwd).toBe(process.cwd());

        const config = yield* client.configRead(sessionId);
        expect((config.models ?? []).length).toBeGreaterThan(0);

        const checkouts = yield* client.workspaceGitCheckouts({
          repoLocalPaths: [process.cwd()],
          sessionCwd: process.cwd(),
        });
        expect(checkouts.length).toBe(1);
        expect(checkouts[0]?.ok).toBe(true);

        const account = yield* client.accountRead(sessionId);
        expect(["ready", "missing_key", "unauthorized", "unavailable"]).toContain(account.status);

        yield* client.sessionDelete(sessionId);
      }).pipe(
        Effect.catchCause((cause) => {
          const squashed = Cause.squash(cause);
          if (isProtocolError(squashed) && squashed.detail.startsWith("skip:")) {
            return Effect.logWarning(squashed.detail);
          }
          return Effect.failCause(cause);
        }),
        Effect.provide(NodeServices.layer),
      ),
    ),
  );

  /**
   * The goal lifecycle against a real server, never `status: "active"`: an active
   * goal starts real continuation turns, so the smoke test manages it paused only.
   */
  it.effect("goal get, set, edit and clear on an interactive ephemeral session", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const child = yield* spawner
          .spawn(ChildProcess.make("vibe-app-server", [], { cwd: process.cwd() }))
          .pipe(
            Effect.catchCause((cause) =>
              Effect.fail(
                new VibeError.VibeAppServerProtocolError({
                  detail: `skip: vibe-app-server is not installed (${String(Cause.squash(cause))})`,
                  cause,
                }),
              ),
            ),
          );
        const client = Context.get(
          yield* Layer.build(VibeClient.layerChildProcess(child)),
          VibeClient.VibeAppServerClient,
        );

        yield* client.initialize({ clientName: "t3-test", clientVersion: "0.0.0" });
        yield* client.initialized();

        // Goals need an interactive session; headless data sessions refute even goal/get.
        const started = yield* client.sessionStart({
          kind: "ephemeral",
          agentConfig: { cwd: process.cwd(), headless: false, trustWorkspace: false },
        });
        const sessionId = started.state.session.id;
        expect(started.state.session.goal).toBeUndefined();

        expect(yield* client.goalGet(sessionId)).toBe(null);

        const created = yield* client.goalSet({
          sessionId,
          objective: "Live smoke test goal, never resumed",
          status: "paused",
        });
        expect(created.status).toBe("paused");
        expect(created.objective).toBe("Live smoke test goal, never resumed");
        expect(created.revision).toBe(1);

        const edited = yield* client.goalSet({
          sessionId,
          expectedGoalId: created.goalId,
          objective: "Live smoke test goal, edited",
        });
        expect(edited.objective).toBe("Live smoke test goal, edited");
        expect(edited.objectiveRevision).toBeGreaterThan(created.objectiveRevision);

        const read = yield* client.goalGet(sessionId);
        expect(read?.goalId).toBe(created.goalId);

        expect(
          yield* client.goalClear({ sessionId, expectedGoalId: read?.goalId ?? created.goalId }),
        ).toBe(true);
        expect(yield* client.goalGet(sessionId)).toBe(null);

        yield* client.sessionDelete(sessionId);
      }).pipe(
        Effect.catchCause((cause) => {
          const squashed = Cause.squash(cause);
          if (isProtocolError(squashed) && squashed.detail.startsWith("skip:")) {
            return Effect.logWarning(squashed.detail);
          }
          return Effect.failCause(cause);
        }),
        Effect.provide(NodeServices.layer),
      ),
    ),
  );
});
