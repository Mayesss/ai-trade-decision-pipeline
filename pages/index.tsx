import type { GetServerSideProps } from "next";

// The combined swing + scalp dashboard lives at /legacy (defaults to swing
// mode). Root redirects there so the swing UI is the landing page. The prior
// scalp-composer dashboard that lived here is preserved in git history if it's
// ever needed again at a dedicated path.
export const getServerSideProps: GetServerSideProps = async () => ({
  redirect: { destination: "/legacy", permanent: false },
});

export default function IndexRedirect() {
  return null;
}
