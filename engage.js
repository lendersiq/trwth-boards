const dbName = 'UserEngagementDB';
const dbVersion = 1;
let db, currentUser = { uuid: null, name: null };

function initIndexedDB() {
  const request = indexedDB.open(dbName, dbVersion);
  request.onupgradeneeded = (event) => {
    db = event.target.result;
    db.createObjectStore('users', { keyPath: 'uuid' });
    const interactionStore = db.createObjectStore('interactions', { keyPath: 'id', autoIncrement: true });
    interactionStore.createIndex('uuid', 'uuid', { unique: false });
  };
  request.onsuccess = (event) => {
    db = event.target.result;
    checkLocalUser();
  };
  request.onerror = () => showError('Failed to initialize database');
}

function showError(message) {
  const errorDiv = document.createElement('div');
  errorDiv.className = 'error-message';
  errorDiv.textContent = message;
  document.body.appendChild(errorDiv);
  document.getElementById('boardGrid').style.display = 'none';
  document.getElementById('userId').textContent = 'Access Denied';
  document.getElementById('onlineIndicator').className = 'online-indicator offline';
}

function checkLocalUser() {
  const transaction = db.transaction(['users'], 'readonly');
  const store = transaction.objectStore('users');
  store.getAll().onsuccess = (event) => {
    const users = event.target.result;
    const validUser = users.find(user => user.uuid && user.name && typeof user.name === 'string' && user.name.trim() !== '');
    if (validUser) {
      currentUser = { uuid: validUser.uuid, name: validUser.name };
      updateUserStatus(currentUser);
      saveInteraction({ blockId: 'page', type: 'page_open', duration: 0 });
      trackEngagement();
    } else {
      checkInvite();
    }
  };
  transaction.onerror = () => showError('Failed to access local user data. Please try again with a valid invite.');
}

function checkInvite() {
  const urlParams = new URLSearchParams(window.location.search);
  const inviteUUID = urlParams.get('invite');
  if (!inviteUUID) {
    showError('No invitation provided. Please access the page with a valid invite link.');
    return;
  }
  validateUUID(inviteUUID);
}

function validateUUID(uuid) {
  fetch('https://trwth.com/engage/validate-uuid.php', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ uuid })
  })
  .then(res => res.json())
  .then(data => {
    if (data.valid && data.name && typeof data.name === 'string' && data.name.trim() !== '') {
      currentUser = { uuid: uuid, name: data.name };
      saveUser(currentUser);
      updateUserStatus(currentUser);
      saveInteraction({ blockId: 'page', type: 'page_open', duration: 0 });
      trackEngagement();
    } else {
      showError('Invalid or expired invitation. Please request a new invite link.');
    }
  })
  .catch(() => showError('Failed to validate invitation. Please try again later.'));
}

function saveUser(user) {
  const transaction = db.transaction(['users'], 'readwrite');
  const store = transaction.objectStore('users');
  store.put({ uuid: user.uuid, name: user.name, createdAt: new Date() });
}

function updateUserStatus(user) {
  const status = document.getElementById('userId');
  status.textContent = `User: ${user.name}`;
  const indicator = document.getElementById('onlineIndicator');
  indicator.className = 'online-indicator ' + (navigator.onLine ? 'online' : 'offline');
}

function saveInteraction(interaction) {
  const transaction = db.transaction(['interactions'], 'readwrite');
  const store = transaction.objectStore('interactions');
  store.add({
    uuid: currentUser.uuid,
    blockId: interaction.blockId,
    type: interaction.type,
    duration: interaction.duration || 0,
    timestamp: new Date(),
    synced: false
  });
}

function trackEngagement() {
  const cards = document.querySelectorAll('.board-card');
  const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      const blockId = entry.target.dataset.blockId;
      if (entry.isIntersecting) {
        entry.target.dataset.startTime = performance.now();
      } else if (entry.target.dataset.startTime) {
        const duration = performance.now() - entry.target.dataset.startTime;
        saveInteraction({ blockId, type: 'view', duration });
      }
    });
  }, { threshold: 0.5 });

  cards.forEach(card => {
    const body = card.querySelector('.board-card__body');
    // View tracking
    observer.observe(card);
    // Click tracking
    card.addEventListener('click', () => {
      saveInteraction({ blockId: card.dataset.blockId, type: 'click', duration: 0 });
    });
    // Scroll tracking
    body.addEventListener('scroll', () => {
      if (body.dataset.lastScrollTop !== undefined && body.scrollTop !== parseFloat(body.dataset.lastScrollTop)) {
        saveInteraction({ blockId: card.dataset.blockId, type: 'scroll', duration: 0 });
      }
      body.dataset.lastScrollTop = body.scrollTop;
    });
    // Hover tracking
    let hoverStart;
    card.addEventListener('mouseenter', () => {
      hoverStart = performance.now();
    });
    card.addEventListener('mouseleave', () => {
      if (hoverStart) {
        const duration = performance.now() - hoverStart;
        saveInteraction({ blockId: card.dataset.blockId, type: 'hover', duration });
        hoverStart = null;
      }
    });
    // Focus tracking
    card.addEventListener('focus', () => {
      saveInteraction({ blockId: card.dataset.blockId, type: 'focus', duration: 0 });
    });
  });
}

function syncData() {
  if (!navigator.onLine || !currentUser.uuid || !currentUser.name) return;
  const transaction = db.transaction(['interactions'], 'readwrite');
  const store = transaction.objectStore('interactions');
  store.getAll().onsuccess = (event) => {
    const interactions = event.target.result.filter(i => !i.synced);
    if (!interactions.length) return;
    fetch('https://trwth.com/engage/save-interactions.php', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ uuid: currentUser.uuid, interactions })
    })
    .then(res => res.json())
    .then(data => {
      if (data.success) {
        const transaction = db.transaction(['interactions'], 'readwrite');
        const store = transaction.objectStore('interactions');
        interactions.forEach(interaction => {
          store.put({ ...interaction, synced: true });
        });
      }
    })
    .catch(() => console.log('Sync failed, keeping interactions in IndexedDB'));
  };
}

window.addEventListener('online', () => currentUser.uuid && currentUser.name && updateUserStatus(currentUser));
window.addEventListener('offline', () => currentUser.uuid && currentUser.name && updateUserStatus(currentUser));
window.addEventListener('beforeunload', syncData);
setInterval(syncData, 60000); // Sync every minute
initIndexedDB();