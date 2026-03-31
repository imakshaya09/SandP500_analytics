const statusEl = document.getElementById('status');
const tabsEl = document.getElementById('topic-tabs');
const boardEl = document.getElementById('message-board');
const RECENT_COMMENTS_TOPIC = 'recent_comments';
let currentTopic = null;

async function fetchJson(url, options = {}) {
  const res = await fetch(url, options);
  if (!res.ok) {
    const error = await res.text();
    throw new Error(error || res.statusText);
  }
  return res.json();
}

async function loadTopics() {
  try {
    const topics = await fetchJson('/api/topics');
    if (!topics.length) {
      statusEl.textContent = 'No topics available.';
      return;
    }

    tabsEl.innerHTML = '';
    topics.forEach((topic, index) => {
      const button = document.createElement('button');
      button.className = 'tab';
      button.textContent = topic.label;
      button.addEventListener('click', () => selectTopic(topic.topic, button));
      tabsEl.appendChild(button);
      if (index === 0) {
        selectTopic(topic.topic, button);
      }
    });

    statusEl.textContent = 'Select a topic tab to view messages.';
  } catch (error) {
    statusEl.textContent = 'Unable to load topics: ' + error.message;
  }
}

function selectTopic(topic, button) {
  currentTopic = topic;
  document.querySelectorAll('.tab').forEach((tab) => tab.classList.remove('active'));
  button.classList.add('active');
  loadMessages(topic);
}

async function loadMessages(topic) {
  const isRecent = topic === RECENT_COMMENTS_TOPIC;
  statusEl.textContent = isRecent ? 'Loading recent comments...' : `Loading next message for ${topic}...`;
  try {
    const messages = await fetchJson(`/api/messages/${encodeURIComponent(topic)}`);
    renderMessages(messages, isRecent);
    if (messages.length) {
      statusEl.textContent = isRecent
        ? `Showing ${messages.length} recent commented record(s).`
        : `Showing next message for ${topic}.`;
    } else {
      statusEl.textContent = isRecent
        ? 'No recent comments yet.'
        : `No messages left for ${topic}.`;
    }
  } catch (error) {
    boardEl.innerHTML = '<p>Unable to load messages.</p>';
    statusEl.textContent = error.message;
  }
}

function renderMessages(messages, isRecent) {
  if (!messages.length) {
    boardEl.innerHTML = isRecent
      ? '<p>No recent comments yet.</p>'
      : '<p>No messages left for this topic.</p>';
    return;
  }

  if (isRecent) {
    const rows = messages.map((msg) => {
      return `
        <tr>
          <td>${msg.Date}</td>
          <td>${msg.Ticker}</td>
          <td>${msg.diff.toFixed(4)}</td>
          <td>${msg.user_comment || '<em>No comment</em>'}</td>
          <td>${msg.commented_at || '<em>unknown</em>'}</td>
        </tr>
      `;
    }).join('');

    boardEl.innerHTML = `
      <table>
        <thead>
          <tr>
            <th>Date</th>
            <th>Ticker</th>
            <th>Difference</th>
            <th>Comment</th>
            <th>Commented At</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    `;
    return;
  }

  const msg = messages[0];
  const comment = msg.user_comment || '';
  boardEl.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Date</th>
          <th>Ticker</th>
          <th>Difference</th>
          <th>Saved Comment</th>
          <th>New Comment</th>
          <th>Action</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>${msg.Date}</td>
          <td>${msg.Ticker}</td>
          <td>${msg.diff.toFixed(4)}</td>
          <td>${comment ? comment : '<em>No comment</em>'}</td>
          <td>
            <input class="comment-input" id="input-${msg.record_id}" value="${comment.replace(/"/g, '&quot;')}" placeholder="Enter comment" />
          </td>
          <td>
            <button class="save-button" onclick="saveComment(${msg.record_id})">Save</button>
          </td>
        </tr>
      </tbody>
    </table>
  `;
}

async function saveComment(recordId) {
  const input = document.getElementById(`input-${recordId}`);
  const comment = input.value.trim();
  try {
    await fetchJson('/api/comment', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ record_id: recordId, comment }),
    });
    if (currentTopic) {
      loadMessages(currentTopic);
    }
    statusEl.textContent = 'Comment saved.';
  } catch (error) {
    statusEl.textContent = `Unable to save comment: ${error.message}`;
  }
}

window.addEventListener('DOMContentLoaded', loadTopics);
