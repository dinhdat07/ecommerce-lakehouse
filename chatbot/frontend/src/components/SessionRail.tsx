import type { SessionSummary } from "../lib/types";

type Props = {
  sessions: SessionSummary[];
  activeSessionId: string | null;
  onSelect: (sessionId: string) => void;
  onCreate: () => void;
};

export function SessionRail({ sessions, activeSessionId, onSelect, onCreate }: Props) {
  return (
    <aside className="rounded-[28px] bg-ink px-5 py-5 text-white shadow-panel">
      <div className="mb-5 flex items-center justify-between">
        <div>
          <p className="font-display text-xl">Lakehouse Analyst</p>
          <p className="text-xs text-white/70">Internal demo copilot</p>
        </div>
        <button
          className="rounded-full bg-accent px-4 py-2 text-sm font-semibold text-white transition hover:bg-[#bd5727]"
          onClick={onCreate}
        >
          New chat
        </button>
      </div>

      <div className="space-y-2">
        {sessions.map((session) => {
          const active = session.id === activeSessionId;
          return (
            <button
              key={session.id}
              className={`w-full rounded-2xl px-4 py-3 text-left transition ${
                active ? "bg-white/18" : "bg-white/6 hover:bg-white/12"
              }`}
              onClick={() => onSelect(session.id)}
            >
              <p className="line-clamp-2 text-sm font-semibold">{session.title || "New conversation"}</p>
              <p className="mt-1 text-xs text-white/65">
                {new Date(session.updated_at).toLocaleString()}
              </p>
            </button>
          );
        })}
      </div>
    </aside>
  );
}
