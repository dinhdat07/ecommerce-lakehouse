import type { SemanticExample } from "../lib/types";

type Props = {
  examples: SemanticExample[];
  onSelect: (question: string) => void;
};

export function StarterPrompts({ examples, onSelect }: Props) {
  return (
    <section className="rounded-[28px] bg-white/85 p-5 shadow-panel">
      <div className="mb-3">
        <p className="font-display text-2xl text-ink">Ask a business question</p>
        <p className="text-sm text-ink/70">
          This copilot answers curated Gold-layer analytics questions in English.
        </p>
      </div>
      <div className="grid gap-3 md:grid-cols-2">
        {examples.map((example) => (
          <button
            key={example.id}
            className="rounded-2xl border border-ink/10 bg-shell px-4 py-4 text-left transition hover:border-accent/50 hover:bg-white"
            onClick={() => onSelect(example.question)}
          >
            <p className="font-semibold text-ink">{example.question}</p>
            <p className="mt-2 text-sm text-ink/70">{example.description}</p>
          </button>
        ))}
      </div>
    </section>
  );
}
