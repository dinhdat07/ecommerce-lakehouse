import { FormEvent, useState } from "react";

type Props = {
  onSubmit: (message: string) => Promise<void>;
  disabled?: boolean;
  status: string | null;
};

export function Composer({ onSubmit, disabled, status }: Props) {
  const [value, setValue] = useState("");

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const message = value.trim();
    if (!message || disabled) return;
    setValue("");
    await onSubmit(message);
  }

  return (
    <form className="rounded-[32px] bg-white p-4 shadow-panel" onSubmit={handleSubmit}>
      <label className="mb-3 block text-xs uppercase tracking-[0.22em] text-ink/45">
        Ask in English
      </label>
      <textarea
        className="min-h-28 w-full resize-none rounded-3xl border border-ink/10 bg-shell px-5 py-4 text-[15px] text-ink outline-none transition focus:border-accent"
        placeholder="Example: Which categories generated the most revenue in the latest available period?"
        value={value}
        onChange={(event) => setValue(event.target.value)}
        disabled={disabled}
      />
      <div className="mt-4 flex items-center justify-between">
        <div className="text-sm text-ink/65">
          {status ? `Working: ${status.replace("_", " ")}` : "Gold-layer analytics only"}
        </div>
        <button
          type="submit"
          disabled={disabled}
          className="rounded-full bg-accent px-5 py-3 text-sm font-semibold text-white transition hover:bg-[#bd5727] disabled:cursor-not-allowed disabled:bg-accent/50"
        >
          Send question
        </button>
      </div>
    </form>
  );
}
