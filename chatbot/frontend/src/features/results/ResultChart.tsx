import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

type Props = {
  rows: Array<Record<string, unknown>>;
  chart: {
    type: string;
    xKey?: string;
    yKeys?: string[];
    seriesKey?: string;
    valueKey?: string;
  } | null | undefined;
};

export function ResultChart({ rows, chart }: Props) {
  if (!chart || rows.length === 0) return null;

  if (chart.type === "line" && chart.xKey && chart.yKeys?.length) {
    return (
      <div className="h-72 rounded-3xl bg-[#f7fafb] p-4">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={rows}>
            <CartesianGrid stroke="#dbe4ea" strokeDasharray="3 3" />
            <XAxis dataKey={chart.xKey} />
            <YAxis />
            <Tooltip />
            {chart.yKeys.map((key, index) => (
              <Line
                key={key}
                type="monotone"
                dataKey={key}
                stroke={index === 0 ? "#d76831" : "#6b7c58"}
                strokeWidth={2}
                dot={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
    );
  }

  if (chart.type === "bar" && chart.xKey && chart.yKeys?.length) {
    const yKey = chart.yKeys[0];
    return (
      <div className="h-72 rounded-3xl bg-[#f7fafb] p-4">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={rows}>
            <CartesianGrid stroke="#dbe4ea" strokeDasharray="3 3" />
            <XAxis dataKey={chart.xKey} />
            <YAxis />
            <Tooltip />
            <Bar dataKey={yKey} radius={[8, 8, 0, 0]}>
              {rows.map((_, index) => (
                <Cell key={index} fill={index % 2 === 0 ? "#d76831" : "#6b7c58"} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    );
  }

  if (chart.type === "heatmap" && chart.xKey && chart.seriesKey && chart.valueKey) {
    return (
      <div className="overflow-auto rounded-3xl bg-[#f7fafb] p-4">
        <div className="grid min-w-[540px] gap-2">
          {rows.slice(0, 80).map((row, index) => {
            const value = Number(row[chart.valueKey!] ?? 0);
            const opacity = Math.min(0.15 + Math.abs(value) / 5, 1);
            return (
              <div
                key={index}
                className="grid grid-cols-[1fr_1fr_120px] items-center rounded-2xl px-3 py-2 text-sm"
                style={{ backgroundColor: `rgba(215, 104, 49, ${opacity})` }}
              >
                <span>{String(row[chart.xKey!])}</span>
                <span>{String(row[chart.seriesKey!])}</span>
                <span className="text-right font-semibold">{value.toFixed(2)}</span>
              </div>
            );
          })}
        </div>
      </div>
    );
  }

  return null;
}
