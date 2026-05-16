import type { SessionSummary } from "../lib/types";

type Props = {
  sessions: SessionSummary[];
  activeSessionId: string | null;
  onSelect: (sessionId: string) => void;
  onCreate: () => void;
};

export function SessionRail({ sessions, activeSessionId, onSelect, onCreate }: Props) {
  return (
    <aside className="h-full bg-ink px-4 py-6 text-white flex flex-col">
      <div className="mb-6 flex flex-col gap-4">
        <div>
          <p className="font-display text-2xl font-semibold">Data Insights</p>
          <p className="text-sm text-white/70">Internal Copilot</p>
        </div>
        <button
          className="w-full rounded-xl bg-accent px-4 py-3 text-sm font-semibold text-white transition hover:bg-opacity-90 flex items-center justify-center gap-2 shadow-sm"
          onClick={onCreate}
        >
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
            <path d="M12 5V19M5 12H19" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
          </svg>
          New Chat
        </button>
      </div>

      <div className="flex-1 overflow-y-auto space-y-1 mt-4">
        <p className="text-xs font-semibold uppercase tracking-wider text-white/50 mb-3 px-2">Conversations</p>
        {sessions.map((session) => {
          const active = session.id === activeSessionId;
          return (
            <button
              key={session.id}
              className={`w-full rounded-lg px-3 py-3 text-left transition flex flex-col ${
                active ? "bg-white/10" : "hover:bg-white/5"
              }`}
              onClick={() => onSelect(session.id)}
            >
              <p className="line-clamp-1 text-sm font-medium">{session.title || "New conversation"}</p>
              <p className="mt-1 text-[11px] text-white/50">
                {new Date(session.updated_at).toLocaleDateString()}
              </p>
            </button>
          );
        })}
      </div>
    </aside>
  );
}
