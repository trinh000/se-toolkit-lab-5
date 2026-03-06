import { useState } from 'react';
import { Dashboard } from './Dashboard';

function Items() {
  return (
    <div>
      <h2>Items Page</h2>
      <p>Здесь был ваш старый код</p>
    </div>
  );
}

function App() {
  const [page, setPage] = useState('items');

  return (
    <div>
      <nav style={{ padding: '10px', borderBottom: '1px solid #ccc' }}>
        <button onClick={() => setPage('items')} style={{ marginRight: '10px' }}>
          Items
        </button>
        <button onClick={() => setPage('dashboard')}>
          Dashboard
        </button>
      </nav>

      <main style={{ padding: '20px' }}>
        {page === 'items' ? <Items /> : <Dashboard />}
      </main>
    </div>
  );
}

export default App;