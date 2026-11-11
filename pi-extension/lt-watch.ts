/**
 * lt-watch: wake a Pi session when another actor changes the shared tracker.
 *
 * Polls `GET /events?after=<cursor>` in-process; no model call happens unless
 * an event by another actor arrives. Enabled per session by
 * ~/.config/llm-tasks/watch.json:
 *
 *   { "<pi-session-id>": { "actor": "agent-server-wow-ui-sim" } }
 *
 * Optional per-session keys: "url" (default https://tasks.sakuin.org),
 * "intervalSeconds" (default 60). Token: ~/.config/llm-tasks/token.
 * Cursor: ~/.config/llm-tasks/watch-cursor-<session-id>.
 */

import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

interface WatchConfig {
	actor: string;
	url?: string;
	intervalSeconds?: number;
}

interface TrackerEvent {
	id: number;
	task_id: string;
	actor: string;
	action: string;
	field?: string | null;
	new_value?: string | null;
}

const configDir = join(homedir(), ".config", "llm-tasks");
const maxListedEvents = 20;

function readWatchConfig(sessionId: string): WatchConfig | undefined {
	const path = join(configDir, "watch.json");
	if (!existsSync(path)) return undefined;
	const all = JSON.parse(readFileSync(path, "utf8")) as Record<string, WatchConfig>;
	return all[sessionId];
}

function describeEvent(event: TrackerEvent): string {
	if (event.action === "commented") return `${event.task_id}: comment #${event.new_value} by ${event.actor}`;
	const field = event.field ? ` ${event.field}` : "";
	const value = event.new_value ? ` -> ${event.new_value}` : "";
	return `${event.task_id}: ${event.action}${field}${value} by ${event.actor}`;
}

function formatWake(events: TrackerEvent[]): string {
	const lines = events.slice(0, maxListedEvents).map(describeEvent);
	if (events.length > maxListedEvents) lines.push(`... and ${events.length - maxListedEvents} more`);
	return [
		"Shared tracker update (lt-watch):",
		...lines,
		"Read the affected tasks with `lt show <id>` and act per the task-tracker rule.",
	].join("\n");
}

export default function (pi: ExtensionAPI) {
	let timer: ReturnType<typeof setInterval> | undefined;

	pi.on("session_start", async (_event, ctx) => {
		const sessionId = ctx.sessionManager.getSessionId();
		const config = readWatchConfig(sessionId);
		if (!config) return;

		const url = config.url ?? "https://tasks.sakuin.org";
		const token = readFileSync(join(configDir, "token"), "utf8").trim();
		const cursorPath = join(configDir, `watch-cursor-${sessionId}`);
		let cursor = existsSync(cursorPath) ? Number(readFileSync(cursorPath, "utf8").trim()) : undefined;
		let lastError = "";
		let polling = false;

		const fetchEvents = async (after: number): Promise<TrackerEvent[]> => {
			const response = await fetch(`${url}/events?after=${after}`, {
				headers: { Authorization: `Bearer ${token}`, "User-Agent": "lt-watch" },
				signal: AbortSignal.timeout(15_000),
			});
			if (!response.ok) throw new Error(`GET /events: HTTP ${response.status}`);
			return (await response.json()) as TrackerEvent[];
		};

		const poll = async () => {
			if (polling) return;
			polling = true;
			try {
				const events = await fetchEvents(cursor ?? 0);
				const isFirstRun = cursor === undefined;
				if (events.length > 0) cursor = events[events.length - 1].id;
				else cursor ??= 0;
				const foreign = events.filter((event) => event.actor !== config.actor);
				// First run only establishes the cursor: history is not news.
				if (!isFirstRun && foreign.length > 0) {
					pi.sendMessage(
						{ customType: "lt-watch", content: formatWake(foreign), display: true },
						{ triggerTurn: true, deliverAs: "followUp" },
					);
				}
				writeFileSync(cursorPath, `${cursor}\n`);
				lastError = "";
			} catch (error) {
				const message = error instanceof Error ? error.message : String(error);
				if (message !== lastError && ctx.hasUI) ctx.ui.notify(`lt-watch: ${message}`, "warning");
				lastError = message;
			} finally {
				polling = false;
			}
		};

		await poll();
		timer = setInterval(poll, (config.intervalSeconds ?? 60) * 1000);
		if (ctx.hasUI) ctx.ui.notify(`lt-watch: watching ${url} as ${config.actor}`, "info");
	});

	pi.on("session_shutdown", async () => {
		if (timer) clearInterval(timer);
		timer = undefined;
	});
}
