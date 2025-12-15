import type { AppProps } from 'next/app';
import Head from 'next/head';

import '../styles/globals.css';

export default function App({ Component, pageProps }: AppProps) {
  return (
    <>
      <Head>
        <link rel="icon" href="/favicon.svg?v=2" type="image/svg+xml" />
      </Head>
      <Component {...pageProps} />
    </>
  );
}
