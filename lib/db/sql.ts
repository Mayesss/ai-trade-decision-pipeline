export type SqlFragment = {
  text: string;
  values: unknown[];
};

function isSqlFragment(value: unknown): value is SqlFragment {
  return (
    Boolean(value) &&
    typeof value === "object" &&
    typeof (value as SqlFragment).text === "string" &&
    Array.isArray((value as SqlFragment).values)
  );
}

function shiftPlaceholders(text: string, offset: number): string {
  if (!offset) return text;
  return text.replace(/\$(\d+)/g, (_full, idx) => {
    const n = Number(idx);
    if (!Number.isFinite(n) || n <= 0) return `$${idx}`;
    return `$${n + offset}`;
  });
}

function appendFragment(
  out: SqlFragment,
  value: SqlFragment | unknown,
): SqlFragment {
  if (isSqlFragment(value)) {
    const shifted = shiftPlaceholders(value.text, out.values.length);
    out.text += shifted;
    out.values.push(...value.values);
    return out;
  }
  out.values.push(value);
  out.text += `$${out.values.length}`;
  return out;
}

export function raw(text: string): SqlFragment {
  return {
    text: String(text || ""),
    values: [],
  };
}

export const empty: SqlFragment = raw("");

export function sql(
  strings: TemplateStringsArray | readonly string[],
  ...values: unknown[]
): SqlFragment {
  const chunks = Array.isArray(strings) ? strings : [];
  const out: SqlFragment = {
    text: "",
    values: [],
  };
  for (let idx = 0; idx < chunks.length; idx += 1) {
    out.text += chunks[idx] || "";
    if (idx < values.length) {
      appendFragment(out, values[idx]);
    }
  }
  return out;
}

export function join(
  values: readonly unknown[],
  separator: string | SqlFragment = ", ",
): SqlFragment {
  if (!Array.isArray(values) || values.length === 0) return empty;
  const out: SqlFragment = {
    text: "",
    values: [],
  };
  const sep = typeof separator === "string" ? raw(separator) : separator;
  for (let idx = 0; idx < values.length; idx += 1) {
    if (idx > 0) appendFragment(out, sep);
    appendFragment(out, values[idx]);
  }
  return out;
}
