type Props = {
  columns: string[];
  rows: Array<Record<string, unknown>>;
};

export function ResultTable({ columns, rows }: Props) {
  if (!columns.length || !rows.length) return null;
  return (
    <div className="overflow-auto rounded-3xl border border-ink/10">
      <table className="min-w-full border-collapse text-sm">
        <thead className="bg-ink text-white">
          <tr>
            {columns.map((column) => (
              <th key={column} className="px-4 py-3 text-left font-semibold">
                {column}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, rowIndex) => (
            <tr key={rowIndex} className={rowIndex % 2 === 0 ? "bg-white" : "bg-shell/60"}>
              {columns.map((column) => (
                <td key={column} className="border-t border-ink/5 px-4 py-3 align-top text-ink/85">
                  {String(row[column] ?? "")}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
