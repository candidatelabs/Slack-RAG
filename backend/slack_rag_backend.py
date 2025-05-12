def build_claude_context(self, channel_id=None, start_ts=None, end_ts=None):
    # If no start_ts or end_ts provided, use the full timeline
    if start_ts is None:
        start_ts = 0
    if end_ts is None:
        end_ts = float('inf')

    # Fetch all messages in the date range
    messages = self.data_store.get_messages(channel_id=channel_id, start_ts=start_ts, end_ts=end_ts)
    candidates = self.candidate_extractor.extract_candidates_from_messages(messages)
    context_blocks = []

    # Group messages by channel
    channel_messages = {}
    for message in messages:
        channel_id = message.get('channel_id')
        if channel_id not in channel_messages:
            channel_messages[channel_id] = []
        channel_messages[channel_id].append(message)

    # For each channel, build a context block
    for channel_id, channel_msgs in channel_messages.items():
        channel_name = self.data_store.get_channel_name(channel_id)
        context_blocks.append(f"Channel: {channel_name}")

        # Track parent messages we've already included to avoid duplicates
        included_parents = set()

        # First, process all thread replies to ensure we have their parents
        for message in channel_msgs:
            if message.get('thread_ts') and message.get('ts') != message.get('thread_ts'):
                parent_ts = message.get('thread_ts')
                if parent_ts not in included_parents:
                    parent_message = self.data_store.get_message_by_ts(parent_ts)
                    if parent_message:
                        context_blocks.append(f"Parent Message: {parent_message.get('text')}")
                        included_parents.add(parent_ts)

        # Then process all messages and their thread replies
        for message in channel_msgs:
            # Skip thread replies as they'll be included with their parent
            if message.get('thread_ts') and message.get('ts') != message.get('thread_ts'):
                continue

            context_blocks.append(f"Message: {message.get('text')}")
            
            # Get all thread replies for this message
            thread_replies = self.data_store.get_thread_replies(message.get('ts'))
            if thread_replies:
                context_blocks.append("Thread Replies:")
                for reply in thread_replies:
                    reply_user = self._get_user_name(reply.get('user'))
                    context_blocks.append(f"- {reply_user}: {reply.get('text')}")

    return "\n".join(context_blocks) 