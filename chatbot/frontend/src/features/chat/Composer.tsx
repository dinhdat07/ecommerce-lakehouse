import { FormEvent, useState } from "react";

type Props = {
  onSubmit: (message: string) => Promise<void>;
  disabled?: boolean;
  status: string | null;
};

export function Composer({ onSubmit, disabled, status }: Props) {
  const [value, setValue] = useState("");

  async function handleSubmit(event?: FormEvent<HTMLFormElement>) {
    if (event) event.preventDefault();
    const message = value.trim();
    if (!message || disabled) return;
    setValue("");
    await onSubmit(message);
  }

  async function handleKeyDown(e: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      const event = e as unknown as FormEvent<HTMLFormElement>;
      await handleSubmit(event);
    }
  }

  return (
    <form 
      className="rounded-2xl bg-white p-2 shadow-panel border border-ink/5 flex items-end gap-2 transition-all focus-within:border-accent/50 focus-within:ring-2 focus-within:ring-accent/20" 
      onSubmit={handleSubmit}
    >
      <textarea
        className="flex-1 max-h-32 min-h-[44px] w-full resize-none bg-transparent px-4 py-3 text-[15px] text-ink outline-none"
        placeholder="Ask a data question..."
        value={value}
        onChange={(event) => setValue(event.target.value)}
        onKeyDown={handleKeyDown}
        disabled={disabled}
        rows={1}
      />
      <button
        type="submit"
        disabled={disabled || !value.trim()}
        className="mb-1 mr-1 flex h-10 w-10 shrink-0 items-center justify-center rounded-full bg-accent text-white transition hover:bg-[#bd5727] disabled:cursor-not-allowed disabled:bg-accent/50"
      >
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
          <path d="M22 2L11 13M22 2L15 22L11 13M11 13L2 9L22 2" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
        </svg>
      </button>
    </form>
  );
}
