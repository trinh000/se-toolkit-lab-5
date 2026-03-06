import { useState, useEffect } from 'react';
import { Bar } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend
} from 'chart.js';

// Регистрируем компоненты Chart.js
ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend
);

export function Dashboard() {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  // Берем API ключ из localStorage
  const token = localStorage.getItem('api_key');
  const API_URL = import.meta.env.VITE_API_TARGET || 'http://localhost:42002';

  useEffect(() => {
    fetch(`${API_URL}/analytics/scores?lab=lab-04`, {
      headers: { 'Authorization': `Bearer ${token}` }
    })
      .then(res => {
        if (!res.ok) throw new Error('Failed to fetch');
        return res.json();
      })
      .then(data => {
        setData(data);
        setLoading(false);
      })
      .catch(err => {
        setError(err.message);
        setLoading(false);
      });
  }, []);

  if (loading) return <div>Loading dashboard...</div>;
  if (error) return <div>Error: {error}</div>;
  if (!data || data.length === 0) return <div>No data available</div>;

  const chartData = {
    labels: data.map((item: any) => item.bucket),
    datasets: [
      {
        label: 'Number of submissions',
        data: data.map((item: any) => item.count),
        backgroundColor: 'rgba(75, 192, 192, 0.6)',
      }
    ]
  };

  const options = {
    responsive: true,
    plugins: {
      legend: {
        position: 'top' as const,
      },
      title: {
        display: true,
        text: 'Score Distribution'
      }
    }
  };

  return (
    <div style={{ padding: '20px' }}>
      <h1>Analytics Dashboard</h1>
      <div style={{ maxWidth: '600px', margin: '0 auto' }}>
        <Bar data={chartData} options={options} />
      </div>
    </div>
  );
}