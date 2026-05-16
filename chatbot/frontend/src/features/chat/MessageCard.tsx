import { sendFeedback } from "../../lib/api";
import type { ChatMessage } from "../../lib/types";
import { ResultChart } from "../results/ResultChart";
import { ResultTable } from "../results/ResultTable";

type Props = {
  message: ChatMessage;
};

export function MessageCard({ message }: Props) {
  if (message.role === "user") {
    return (
      <div className="ml-auto max-w-3xl rounded-[28px] bg-ink px-5 py-4 text-white shadow-panel">
        <p className="text-sm uppercase tracking-[0.22em] text-white/55">You</p>
        <p className="mt-2 whitespace-pre-wrap text-[15px] leading-7">{message.text}</p>
      </div>
    );
  }

  return (
    <div className="max-w-4xl rounded-[32px] bg-white px-6 py-6 shadow-panel">
      <div className="flex items-start justify-between gap-4">
        <div>
          <p className="text-sm uppercase tracking-[0.22em] text-ink/45">Assistant</p>
          <p className="mt-3 whitespace-pre-wrap text-[15px] leading-7 text-ink">{message.text}</p>
        </div>
        <div className="flex gap-2">
          <button
            className="rounded-full border border-ink/10 px-3 py-2 text-xs text-ink/70 hover:border-ink/30"
            onClick={() => void sendFeedback(message.id, "up")}
          >
            Helpful
          </button>
          <button
            className="rounded-full border border-ink/10 px-3 py-2 text-xs text-ink/70 hover:border-ink/30"
            onClick={() => void sendFeedback(message.id, "down")}
          >
            Needs work
          </button>
        </div>
      </div>

      <div className="mt-5 flex flex-wrap gap-3 text-xs text-ink/65">
        {message.confidence != null && (
          <span className="rounded-full bg-shell px-3 py-2">
            Confidence {(message.confidence * 100).toFixed(0)}%
          </span>
        )}
        {message.elapsed_ms != null && (
          <span className="rounded-full bg-shell px-3 py-2">
            Runtime {message.elapsed_ms} ms
          </span>
        )}
        {!!message.row_count && (
          <span className="rounded-full bg-shell px-3 py-2">{message.row_count} rows</span>
        )}
        {message.tables_used.map((table) => (
          <span key={table} className="rounded-full bg-mist px-3 py-2 text-ink">
            {table}
          </span>
        ))}
      </div>

      {message.warnings.length > 0 && (
        <div className="mt-4 rounded-3xl border border-accent/30 bg-[#fff4ef] px-4 py-3 text-sm text-[#8a471f]">
          {message.warnings.join(" ")}
        </div>
      )}

      <div className="mt-6 space-y-5">
        <ResultChart rows={message.rows} chart={message.chart_suggestion} />
        <ResultTable columns={message.columns} rows={message.rows} />
        {message.sql && (
          <div className="rounded-3xl bg-[#f7fafb] p-4">
            <p className="mb-2 text-xs uppercase tracking-[0.22em] text-ink/45">SQL used</p>
            <pre className="overflow-auto whitespace-pre-wrap text-sm leading-6 text-ink">{message.sql}</pre>
          </div>
        )}
      </div>
    </div>
  );
}
