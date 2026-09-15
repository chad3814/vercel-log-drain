import { useState } from 'react';
import { Status } from './views/Status.tsx';

type Tab = 'status';

const TABS: { id: Tab; label: string }[] = [{ id: 'status', label: 'Status' }];

export function App(): React.JSX.Element {
  const [tab, setTab] = useState<Tab>('status');

  return (
    <main>
      <h1>Vercel Log Drain</h1>
      <nav>
        {TABS.map((entry) => (
          <button
            key={entry.id}
            type="button"
            aria-current={tab === entry.id}
            onClick={() => setTab(entry.id)}
          >
            {entry.label}
          </button>
        ))}
      </nav>
      {tab === 'status' ? <Status /> : null}
    </main>
  );
}
