// api/health.ts
export const config = { runtime: 'nodejs' };

// Do NOT import any values from @vercel/node, only types
export default function handler(req: any, res: any) {
  if (req.method !== 'GET') {
    res.statusCode = 405;
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify({ error: 'Method Not Allowed', message: 'Use GET' }));
    return;
  }

  // simple health signal
  res.statusCode = 200;
  res.setHeader('Content-Type', 'text/plain');
  res.end('ok');
}