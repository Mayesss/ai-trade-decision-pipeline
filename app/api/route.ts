import { NextResponse } from 'next/server';

export function GET() {
  return NextResponse.json({ message: 'Welcome to the TypeScript API!' });
}
