from datetime import datetime, timedelta
from typing import Dict, List

class NotificationQueue:
    def __init__(self, max_age_hours=72):
        self.queues: Dict[str, List[Dict]] = {}
        self.max_age = max_age_hours

    def add_notification(self, user_id: str, notification: Dict):
        """Add a notification to a user's queue with timestamp"""
        if user_id not in self.queues:
            self.queues[user_id] = []
        
        notification['queued_at'] = datetime.now()
        self.queues[user_id].append(notification)
        self._cleanup_old_notifications(user_id)

    def get_notifications(self, user_id: str) -> List[Dict]:
        """Retrieve and clear notifications for a user"""
        if user_id not in self.queues:
            return []
        
        self._cleanup_old_notifications(user_id)
        notifications = self.queues[user_id]
        self.queues[user_id] = []
        return notifications

    def _cleanup_old_notifications(self, user_id: str):
        """Remove notifications older than max_age"""
        if user_id not in self.queues:
            return
        
        current_time = datetime.now()
        self.queues[user_id] = [
            notif for notif in self.queues[user_id]
            if (current_time - notif['queued_at']) < timedelta(hours=self.max_age)
        ]


notification_queue = NotificationQueue()
offline_notifications: Dict[str, List[Dict[str, any]]] = {}