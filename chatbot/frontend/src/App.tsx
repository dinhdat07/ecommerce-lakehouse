import { useEffect, useState } from "react";
import { Composer } from "./features/chat/Composer";
import { MessageCard } from "./features/chat/MessageCard";
import { SessionRail } from "./components/SessionRail";
import { StarterPrompts } from "./components/StarterPrompts";
import { createSession, getExamples, getSession, listSessions, streamChatMessage } from "./lib/api";
import type { ChatMessage, SemanticExample, SessionDetail, SessionSummary } from "./lib/types";

function App() {
  const [sessions, setSessions] = useState<SessionSummary[]>([]);
  const [activeSessionId, setActiveSessionId] = useState<string | null>(null);
  const [sessionDetail, setSessionDetail] = useState<SessionDetail | null>(null);
  const [examples, setExamples] = useState<SemanticExample[]>([]);
  const [status, setStatus] = useState<string | null>(null);
  const [draftSql, setDraftSql] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    void bootstrap();
  }, []);

  async function bootstrap() {
    try {
      const [loadedSessions, loadedExamples] = await Promise.all([listSessions(), getExamples()]);
      setSessions(loadedSessions);
      setExamples(loadedExamples);
      if (loadedSessions[0]) {
        await openSession(loadedSessions[0].id);
      } else {
        await handleCreateSession();
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to bootstrap the demo.");
    }
  }

  async function handleCreateSession() {
    const session = await createSession();
    setSessions((current) => [session, ...current]);
    setActiveSessionId(session.id);
    setSessionDetail({ ...session, messages: [] });
    setDraftSql(null);
    setError(null);
  }

  async function openSession(sessionId: string) {
    const detail = await getSession(sessionId);
    setActiveSessionId(sessionId);
    setSessionDetail(detail);
    setDraftSql(null);
    setError(null);
  }

  async function submitMessage(message: string) {
    if (!activeSessionId) return;
    const userMessage: ChatMessage = {
      id: `local-${Date.now()}`,
      role: "user",
      text: message,
      created_at: new Date().toISOString(),
      columns: [],
      rows: [],
      tables_used: [],
      warnings: [],
    };
    setSessionDetail((current) =>
      current ? { ...current, messages: [...current.messages, userMessage] } : current,
    );
    setBusy(true);
    setStatus("planning");
    setDraftSql(null);
    setError(null);
    try {
      await streamChatMessage(activeSessionId, message, {
        onStatus: (nextStatus) => setStatus(nextStatus),
        onSql: (sql) => setDraftSql(sql),
        onComplete: (payload) => {
          const completed = payload as ChatMessage;
          setSessionDetail((current) =>
            current ? { ...current, messages: [...current.messages, completed] } : current,
          );
        },
      });
      const refreshedSessions = await listSessions();
      setSessions(refreshedSessions);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Message failed.");
    } finally {
      setBusy(false);
      setStatus(null);
      setDraftSql(null);
    }
  }

  return (
    <div className="min-h-screen bg-[radial-gradient(circle_at_top_left,_#fffefb,_#ebe3d4_45%,_#dbe4ea)] px-4 py-4 text-ink md:px-6">
      <div className="mx-auto grid max-w-[1600px] gap-4 xl:grid-cols-[300px_minmax(0,1fr)_360px]">
        <SessionRail
          sessions={sessions}
          activeSessionId={activeSessionId}
          onSelect={(id) => void openSession(id)}
          onCreate={() => void handleCreateSession()}
        />

        <main className="space-y-4">
          <StarterPrompts examples={examples} onSelect={(question) => void submitMessage(question)} />

          <section className="rounded-[32px] bg-white/70 p-5 shadow-panel backdrop-blur">
            <div className="mb-5 flex items-center justify-between">
              <div>
                <p className="font-display text-3xl">Executive analyst copilot</p>
                <p className="text-sm text-ink/70">
                  Ask revenue, conversion, retention, product, category, or RFM questions.
                </p>
              </div>
            </div>

            <div className="space-y-5">
              {sessionDetail?.messages.length ? (
                sessionDetail.messages.map((message) => <MessageCard key={message.id} message={message} />)
              ) : (
                <div className="rounded-[28px] border border-dashed border-ink/15 bg-shell/60 px-6 py-10 text-center text-ink/65">
                  Start with one of the suggested prompts or ask your own business question.
                </div>
              )}
              {error && (
                <div className="rounded-3xl border border-[#d76831]/30 bg-[#fff4ef] px-4 py-3 text-sm text-[#8a471f]">
                  {error}
                </div>
              )}
            </div>
          </section>

          <Composer onSubmit={submitMessage} disabled={busy} status={status} />
        </main>

        <aside className="space-y-4">
          <section className="rounded-[32px] bg-white p-5 shadow-panel">
            <p className="text-xs uppercase tracking-[0.22em] text-ink/45">Trust panel</p>
            <p className="mt-3 font-display text-2xl">What this demo can do</p>
            <ul className="mt-4 space-y-3 text-sm leading-6 text-ink/75">
              <li>Answers English-language analytics questions against curated Gold tables.</li>
              <li>Shows the SQL it used and the result shape it returned.</li>
              <li>Suggests charts only when the returned data supports them cleanly.</li>
            </ul>
          </section>

          <section className="rounded-[32px] bg-ink p-5 text-white shadow-panel">
            <p className="text-xs uppercase tracking-[0.22em] text-white/55">Live status</p>
            <p className="mt-3 font-display text-2xl">Current run state</p>
            <div className="mt-4 rounded-3xl bg-white/10 p-4 text-sm text-white/80">
              {status ? `Working on: ${status.replace("_", " ")}` : "Idle and ready for the next question."}
            </div>
            {draftSql && (
              <div className="mt-4 rounded-3xl bg-white/8 p-4">
                <p className="mb-2 text-xs uppercase tracking-[0.22em] text-white/55">Draft SQL</p>
                <pre className="overflow-auto whitespace-pre-wrap text-xs leading-6 text-white/85">{draftSql}</pre>
              </div>
            )}
          </section>
        </aside>
      </div>
    </div>
  );
}

export default App;
