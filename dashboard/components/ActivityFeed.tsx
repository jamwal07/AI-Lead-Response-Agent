'use client';

import { motion, AnimatePresence } from 'framer-motion';
import { Phone, AlertTriangle, Clock, Activity, RefreshCw } from 'lucide-react';
import useSWR from 'swr';
import { API_URL } from '../lib/api';

interface ActivityItem {
    id: string; // Log entry ID
    lead_id: string; // Lead ID
    phone: string; // Masked phone
    status: string; // EMERGENCY, STANDARD REQ, or empty
    timestamp: string;
    business?: string;
}

const fetcher = async (url: string) => {
    const res = await fetch(url, { credentials: 'include' });
    if (!res.ok) {
        const error = new Error('An error occurred while fetching activity.');
        (error as any).status = res.status;
        throw error;
    }
    return res.json();
};

export default function ActivityFeed() {
    const { data: activities, error, isLoading, mutate } = useSWR(`${API_URL}/api/activity`, fetcher, {
        refreshInterval: 3000, // Auto-refresh every 3s (Real-time feel)
        shouldRetryOnError: (err: any) => err.status !== 401
    });

    const items: ActivityItem[] = Array.isArray(activities) ? activities : [];

    return (
        <div className="bg-brand-gray border border-brand-border rounded-3xl p-6 h-full min-h-[400px] relative overflow-hidden group/feed flex flex-col">
            {/* Background Glow */}
            <div className="absolute -top-24 -right-24 w-48 h-48 bg-brand-lime/5 rounded-full blur-[60px] pointer-events-none group-hover/feed:bg-brand-lime/10 transition-colors" />

            <h3 className="text-lg font-bold text-white mb-6 flex items-center gap-2 relative z-10">
                <Clock size={18} className="text-brand-lime" />
                <span className="uppercase tracking-tight">Live Activity</span>
                <button
                    onClick={() => mutate()}
                    className="p-1 hover:bg-white/10 rounded-full transition-colors ml-auto text-gray-500 hover:text-brand-lime"
                    title="Refresh Now"
                >
                    <RefreshCw size={14} />
                </button>
            </h3>

            <div className="space-y-3 relative z-10 flex-1 overflow-y-auto pr-2 scrollbar-thin scrollbar-thumb-brand-lime/20 scrollbar-track-transparent">
                <AnimatePresence>
                    {items.map((item, index) => (
                        <motion.div
                            key={item.id}
                            initial={{ opacity: 0, x: -10 }}
                            animate={{ opacity: 1, x: 0 }}
                            transition={{ delay: index * 0.05 }}
                            className="flex items-center gap-4 p-4 rounded-2xl bg-[#0a0a0a] border border-brand-border hover:border-brand-lime/30 group transition-all"
                        >
                            <div className={`p-2.5 rounded-xl border ${item.status === 'EMERGENCY' ? 'bg-red-500/10 border-red-500/20 text-red-400' :
                                'bg-gray-500/10 border-white/5 text-gray-500'
                                }`}>
                                {item.status === 'EMERGENCY' ? <AlertTriangle size={18} /> : <Phone size={18} />}
                            </div>

                            <div className="flex-1 min-w-0">
                                <div className="flex items-center justify-between mb-0.5">
                                    <p className="text-sm font-bold text-white tracking-tight">{item.phone}</p>
                                    <span className="text-[10px] text-gray-600 font-mono">{item.timestamp}</span>
                                </div>
                                <div className="flex items-center justify-between">
                                    <p className="text-xs text-gray-500 truncate max-w-[120px]">{item.business}</p>
                                    <span className={`text-[9px] uppercase font-black tracking-widest ${item.status === 'EMERGENCY' ? 'text-red-400' :
                                        'text-gray-600'
                                        }`}>
                                        {item.status}
                                    </span>
                                </div>
                            </div>
                        </motion.div>
                    ))}
                </AnimatePresence>

                {items.length === 0 && !error && !isLoading && (
                    <div className="flex flex-col items-center justify-center py-10 text-gray-600">
                        <Activity size={32} className="mb-2 opacity-20" />
                        <p className="text-[10px] uppercase font-bold tracking-widest">Waiting for activity...</p>
                    </div>
                )}

                {error && (
                    <div className="flex flex-col items-center justify-center py-10 text-red-400/50">
                        <AlertTriangle size={32} className="mb-2 opacity-20" />
                        <p className="text-[10px] uppercase font-bold tracking-widest">{error.status === 401 ? 'Session Expired' : 'Failed to load'}</p>
                    </div>
                )}

                {isLoading && items.length === 0 && (
                    <div className="flex flex-col items-center justify-center py-10 text-gray-600 animate-pulse">
                        <Activity size={32} className="mb-2 opacity-20" />
                        <p className="text-[10px] uppercase font-bold tracking-widest">Syncing...</p>
                    </div>
                )}
            </div>
        </div>
    );
}

