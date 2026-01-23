
import os
import shutil
import time
from datetime import datetime
from execution.utils.logger import setup_logger

logger = setup_logger("BackupService")

def run_backup():
    """
    Copies the production database to a backup folder.
    Rotates old backups (keeps last 7).
    """
    # 1. Paths
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    data_dir = os.path.join(base_dir, 'data')
    db_file = os.path.join(data_dir, 'plumber.db')
    backup_dir = os.path.join(data_dir, 'backups')
    
    if not os.path.exists(db_file):
        logger.error(f"Cannot backup: {db_file} not found.")
        return
        
    os.makedirs(backup_dir, exist_ok=True)
    
    # 2. Daily Backup Name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(backup_dir, f"plumber_backup_{timestamp}.db")
    
    try:
        shutil.copy2(db_file, backup_path)
        logger.info(f"✅ Database Backup Created: {backup_path}")
        
        # 3. Rotate (Keep last 7)
        backups = sorted([f for f in os.listdir(backup_dir) if f.startswith("plumber_backup")], reverse=True)
        if len(backups) > 7:
            for old_f in backups[7:]:
                os.remove(os.path.join(backup_dir, old_f))
                logger.info(f"🗑️ Rotated old backup: {old_f}")
                
    except Exception as e:
        logger.error(f"Backup Failed: {e}")

if __name__ == "__main__":
    run_backup()
