import { useState } from 'react';
import { Status } from './views/Status.tsx';
import { Drains } from './views/Drains.tsx';
import { Sinks } from './views/Sinks.tsx';

type Tab = 'status' | 'drains' | 'sinks';

const TABS: { id: Tab; label: string }[] = [
  { id: 'status', label: 'Status' },
  { id: 'drains', label: 'Drains' },
  { id: 'sinks', label: 'Sinks' },
];

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
      {tab === 'drains' ? <Drains /> : null}
      {tab === 'sinks' ? <Sinks /> : null}
    </main>
  );
}
