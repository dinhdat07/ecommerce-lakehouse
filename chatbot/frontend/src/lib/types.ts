export type SessionSummary = {
  id: string;
  title: string;
  created_at: string;
  updated_at: string;
};

export type ChatMessage = {
  id: string;
  role: "user" | "assistant" | "system";
  text: string;
  created_at: string;
  sql?: string | null;
  columns: string[];
  rows: Array<Record<string, unknown>>;
  row_count?: number | null;
  tables_used: string[];
  chart_suggestion?: {
    type: string;
    xKey?: string;
    yKeys?: string[];
    seriesKey?: string;
    valueKey?: string;
  } | null;
  warnings: string[];
  confidence?: number | null;
  elapsed_ms?: number | null;
};

export type SessionDetail = SessionSummary & {
  messages: ChatMessage[];
};

export type SemanticExample = {
  id: string;
  question: string;
  description: string;
};
