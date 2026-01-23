'use client';

import { useState, useEffect } from 'react';
import Image from 'next/image';
import { LayoutDashboard, Users, DollarSign, Activity, Settings, Download, MessageSquare } from 'lucide-react';
import KpiCard from '@/components/KpiCard';
import StatusToggle from '@/components/StatusToggle';
import ActivityFeed from '@/components/ActivityFeed';
import DashboardChart from '@/components/DashboardChart';
import LeadsModal from '@/components/LeadsModal';
import Footer from '@/components/Footer';

import useSWR from 'swr';
import { useRouter } from 'next/navigation';
import { API_URL } from '../lib/api';

const fetcher = async (url: string) => {
    try {
        const res = await fetch(url, { credentials: 'include' });
        if (!res.ok) {
            const error = new Error('An error occurred while fetching the data.') as any;
            try {
                error.info = await res.json();
            } catch (e) {
                error.info = { error: 'Unknown error' };
            }
            error.status = res.status;
            throw error;
        }
        return res.json();
    } catch (err: any) {
        // Network errors or fetch failures
        if (err.name === 'TypeError' || err.message.includes('fetch')) {
            console.error('Network error:', err);
            // Return cached data if available, or throw with network error
            throw new Error('Network error. Please check your connection.');
        }
        throw err;
    }
};

export default function Home() {
    const router = useRouter();
    const [isActive, setIsActive] = useState(true); // Default (optimistic)
    const [isLeadsModalOpen, setIsLeadsModalOpen] = useState(false);
    const [businessName, setBusinessName] = useState('');
    const [period, setPeriod] = useState<'week' | 'month' | 'lifetime'>('week');
    const { data, error, mutate: mutateStats } = useSWR(`${API_URL}/api/stats?period=${period}`, fetcher, {
        refreshInterval: (data) => {
            // Adaptive polling: less frequent if no new data
            return data?.lastUpdate ? 30000 : 5000;
        },
        revalidateOnFocus: false,
        revalidateOnReconnect: true,
        shouldRetryOnError: (error: any) => {
            // Don't retry on 4xx errors (client errors)
            return error?.status >= 500;
        },
        errorRetryCount: 3,
        errorRetryInterval: 5000,
        onError: (err: any) => {
            if (err.status !== 401) {
                console.error('API Error:', err);
                // Could show toast notification here
            }
        }
    });

    // Fetch initial AI Active status
    useEffect(() => {
        const fetchSettings = async () => {
            try {
                const res = await fetch(`${API_URL}/api/settings`, { credentials: 'include' });
                if (res.ok) {
                    const settings = await res.json();
                    setIsActive(settings.ai_active);
                }
            } catch (e) {
                console.error("Failed to fetch settings", e);
            }
        };
        fetchSettings();
    }, []);

    const toggleAI = async () => {
        const newState = !isActive;
        setIsActive(newState); // Optimistic UI update

        try {
            await fetch(`${API_URL}/api/settings/toggle_ai`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ active: newState }),
                credentials: 'include'
            });
        } catch (e) {
            console.error("Failed to toggle AI", e);
            setIsActive(!newState); // Revert on error
        }
    };

    useEffect(() => {
        const tenant = localStorage.getItem('tenant_id');
        const name = localStorage.getItem('business_name');
        if (!tenant) {
            router.push('/login');
        } else if (name) {
            setBusinessName(name);
        }
    }, [router]);

    // Handle Auth Errors
    useEffect(() => {
        if (error?.status === 401) {
            localStorage.removeItem('tenant_id');
            localStorage.removeItem('business_name');
            router.push('/login');
        }
    }, [error, router]);

    const kpi = data?.kpi || { leads: 0, success_rate: '0.0%', revenue: 0, estimated_cost: 0 };
    const loading = !data;

    return (
        <main className="min-h-screen bg-brand-black text-white font-sans relative overflow-x-hidden flex flex-col">
            {/* Background Effects */}
            <div className="absolute top-0 left-0 w-full h-full overflow-hidden pointer-events-none">
                <div className="absolute top-[-10%] left-[-10%] w-[40%] h-[40%] bg-brand-lime/5 rounded-full blur-[120px]" />
                <div className="absolute bottom-[-10%] right-[-10%] w-[40%] h-[40%] bg-brand-lime/5 rounded-full blur-[120px]" />
            </div>

            {/* Content Wrapper - flex-grow applies here */}
            <div className="w-full max-w-7xl mx-auto space-y-8 relative z-10 p-4 md:p-8 flex-1 pb-16">
                <header className="flex flex-col md:flex-row md:items-center justify-between gap-4">
                    <div>
                        <div className="relative w-80 h-20 mb-2 -ml-8">
                            <Image
                                src="/logo.jpg?v=2"
                                alt="YourPlumberAI"
                                fill
                                className="object-contain object-left mix-blend-screen opacity-90"
                            />
                        </div>
                        <p className="text-gray-500 text-[10px] uppercase tracking-[0.2em] font-black mt-1">Never miss a lead</p>
                    </div>

                    <div className="flex items-center gap-6">
                        <div className="hidden md:flex gap-3">
                            <span className="flex items-center gap-1.5 px-3 py-1 rounded-full bg-brand-lime/10 border border-brand-lime/20 text-xs font-bold text-brand-lime uppercase tracking-wider">
                                <div className={`w-1.5 h-1.5 rounded-full ${loading ? 'bg-gray-400' : 'bg-brand-lime animate-pulse'}`} />
                                {loading ? 'Connecting...' : 'Systems Online'}
                            </span>
                        </div>

                        <div className="h-8 w-px bg-white/10 hidden md:block" />
                        <StatusToggle isActive={isActive} onToggle={toggleAI} />
                    </div>
                </header>

                <div className="flex flex-col gap-6 -mt-4">
                    {businessName && (
                        <div className="ml-1 mb-2">
                            <p className="text-2xl uppercase font-black tracking-[0.1em] text-brand-lime leading-none drop-shadow-[0_0_12px_rgba(204,255,0,0.8)]">
                                {businessName}
                            </p>
                        </div>
                    )}
                    <div className="flex flex-col sm:flex-row gap-4 sm:items-center w-full sm:w-auto">
                        <button
                            onClick={() => setIsLeadsModalOpen(true)}
                            className="flex items-center justify-center gap-2 px-6 py-3 rounded-full bg-brand-gray hover:bg-[#1a1a1a] border border-brand-border text-xs font-bold uppercase tracking-wide text-gray-300 hover:text-white hover:border-brand-lime/50 transition-all whitespace-nowrap w-full sm:w-auto"
                        >
                            <Users size={16} />
                            View All Leads
                        </button>

                        {/* Time Period Selector */}
                        <div className="flex w-full sm:w-auto bg-brand-gray border border-brand-border rounded-full p-1 gap-1">
                            {(['week', 'month', 'lifetime'] as const).map((p) => (
                                <button
                                    key={p}
                                    onClick={() => setPeriod(p)}
                                    className={`flex-1 sm:flex-none px-4 py-2 rounded-full text-xs font-bold uppercase tracking-wide transition-all text-center whitespace-nowrap ${period === p
                                        ? 'bg-brand-lime text-black shadow-[0_0_10px_rgba(204,255,0,0.3)]'
                                        : 'text-gray-400 hover:text-white hover:bg-white/5'
                                        }`}
                                >
                                    {p === 'week' ? 'This Week' : p === 'month' ? 'This Month' : 'Lifetime'}
                                </button>
                            ))}
                        </div>
                    </div>
                </div>

                </div>

                {/* KPI Grid - Updated to 4 cols for Cost Metric */}
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                    <KpiCard
                        title="Missed Calls Caught"
                        value={loading ? "..." : kpi.leads.toString()}
                        icon={Users}
                        trend={kpi.leads_trend}
                        trendUp={true}
                        delay={0}
                    />
                    <KpiCard
                        title="Success Rate"
                        value={loading ? "..." : kpi.success_rate}
                        icon={Activity}
                        trend="Top 1% performing"
                        trendUp={true}
                        delay={0.1}
                    />
                    <KpiCard
                        title="Revenue Saved"
                        value={loading ? "..." : `$${kpi.revenue.toLocaleString()}`}
                        icon={DollarSign}
                        trend="Est. based on misses"
                        trendUp={true}
                        delay={0.2}
                    />
                    <KpiCard
                        title="Est. MTD Cost"
                        value={loading ? "..." : `$${(kpi.estimated_cost || 0).toFixed(2)}`}
                        icon={Settings}
                        trend="Real-time Usage"
                        trendUp={false} 
                        delay={0.3}
                    />
                </div>

                {/* Main Content Grid - Responsive with min-heights to prevent overlap */}
                <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                    {/* Chart Area - Responsive height */}
                    <div className="lg:col-span-2 min-h-[400px] lg:h-[450px] flex flex-col">
                        <DashboardChart data={data?.chart_data} />
                    </div>

                    {/* Activity Feed - Responsive height with overflow handling */}
                    <div className="min-h-[400px] lg:h-[450px] flex flex-col">
                        <ActivityFeed />
                    </div>
                </div>

                {/* Spacer to guarantee footer separation */}
                <div className="h-12" />
            </div>

            <LeadsModal
                isOpen={isLeadsModalOpen}
                onClose={() => setIsLeadsModalOpen(false)}
            />

            <Footer />
        </main >
    );
}

