
from flask import Blueprint, jsonify, request
from execution.utils import database
from execution.utils.logger import setup_logger

logger = setup_logger("DashboardAPI")
dashboard_bp = Blueprint('dashboard', __name__)

@dashboard_bp.route('/api/activity', methods=['GET'])
def get_activity():
    """
    Returns recent conversation activity for the live feed.
    """
    tenant_id = request.args.get('tenant_id') # Optional filter
    logs = database.get_recent_conversation_logs(limit=20, tenant_id=tenant_id)
    
    # Format for UI
    formatted = []
    for log in logs:
        formatted.append({
            'id': log['id'],
            'lead_id': log['lead_id'],
            'phone': log['lead_phone'],
            'status': log['intent'] if log['intent'] else 'STANDARD',
            'timestamp': log['created_at'],
            'business': log.get('business', 'Lead Activity')
        })
    
    return jsonify(formatted)

@dashboard_bp.route('/api/stats', methods=['GET'])
def get_stats():
    """Returns top-level stats for the dashboard"""
    tenant_id = request.args.get('tenant_id')
    stats = database.get_lead_funnel_stats(tenant_id=tenant_id)
    revenue = database.get_revenue_stats(tenant_id=tenant_id)
    
    return jsonify({
        'leads': stats,
        'revenue': revenue
    })

@dashboard_bp.route('/api/health', methods=['GET'])
def health():
    return jsonify({"status": "ok"})
