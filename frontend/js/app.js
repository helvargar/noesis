/**
 * Noesis AI - Admin Dashboard JavaScript
 * Handles authentication, tenant management, and UI interactions
 */

// ==================== Configuration ====================
const API_BASE = '/api/v1';
let authToken = localStorage.getItem('noesis_token');

// ==================== API Helper ====================
async function api(endpoint, options = {}) {
    const headers = {
        'Content-Type': 'application/json',
        ...options.headers
    };

    if (authToken) {
        headers['Authorization'] = `Bearer ${authToken}`;
    }

    const response = await fetch(`${API_BASE}${endpoint}`, {
        ...options,
        headers
    });

    if (response.status === 401) {
        if (!endpoint.includes('/auth/login')) {
            logout();
            throw new Error('Session expired');
        }
    }

    if (!response.ok) {
        let errorMessage = 'Request failed';
        try {
            const error = await response.json();
            if (error.detail) {
                if (Array.isArray(error.detail)) {
                    errorMessage = error.detail.map(d => `${d.loc.join('.')}: ${d.msg}`).join('; ');
                } else {
                    errorMessage = error.detail;
                }
            }
        } catch (e) { }
        throw new Error(errorMessage);
    }

    return response.json();
}

// ==================== Toast Notifications ====================
function showToast(message, type = 'info') {
    const container = document.getElementById('toast-container');
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;

    const icons = { success: '✓', error: '✕', info: 'ℹ' };
    toast.innerHTML = `<span>${icons[type]}</span><span>${message}</span>`;

    container.appendChild(toast);

    setTimeout(() => {
        toast.style.animation = 'toastIn 0.3s ease reverse';
        setTimeout(() => toast.remove(), 300);
    }, 4000);
}

// ==================== Screen Management ====================
function showScreen(screenId) {
    document.querySelectorAll('.screen').forEach(s => s.classList.remove('active'));
    document.getElementById(screenId).classList.add('active');
}

function showSection(sectionId) {
    document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
    document.getElementById(`${sectionId}-section`).classList.add('active');

    document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
    document.querySelector(`.nav-item[data-section="${sectionId}"]`).classList.add('active');
}

// ==================== Authentication ====================
async function login(email, password) {
    try {
        const data = await api('/auth/login', {
            method: 'POST',
            body: JSON.stringify({ email, password })
        });

        authToken = data.access_token;
        localStorage.setItem('noesis_token', authToken);

        showScreen('dashboard-screen');
        loadTenants();
        showToast('Login effettuato!', 'success');

    } catch (error) {
        throw error;
    }
}

function logout() {
    authToken = null;
    localStorage.removeItem('noesis_token');
    showScreen('login-screen');
}

function checkAuth() {
    if (authToken) {
        showScreen('dashboard-screen');
        loadTenants();
    } else {
        showScreen('login-screen');
    }
}

// ==================== Tenants ====================
let tenants = [];
let editingTenantId = null;
let pendingFiles = [];

async function loadTenants() {
    try {
        tenants = await api('/tenants');
        renderTenants();
    } catch (error) {
        showToast('Errore caricamento tenants', 'error');
    }
}

function renderTenants() {
    const grid = document.getElementById('tenants-grid');

    if (tenants.length === 0) {
        grid.innerHTML = `
            <div class="coming-soon">
                <span class="emoji">🏢</span>
                <p>Nessun tenant configurato</p>
                <p style="color: var(--text-muted)">Clicca "Nuovo Tenant" per iniziare</p>
            </div>
        `;
        return;
    }

    grid.innerHTML = tenants.map(t => `
        <div class="tenant-card" data-id="${t.id}">
            <div class="tenant-card-header">
                <div>
                    <div class="tenant-name">${t.name}</div>
                    <div class="tenant-id">${t.id}</div>
                </div>
                <span class="tenant-status ${t.is_active ? 'active' : 'inactive'}">
                    ${t.is_active ? '● Attivo' : '○ Inattivo'}
                </span>
            </div>
            
            <div class="tenant-badges">
                <span class="badge ${t.llm_provider ? 'enabled' : ''}">
                    🤖 ${t.llm_provider || 'No LLM'}
                </span>
                <span class="badge ${t.db_enabled ? 'enabled' : ''}">
                    🗄️ ${t.db_enabled ? 'DB Configurato' : 'No DB'}
                </span>
                <span class="badge ${t.docs_enabled ? 'enabled' : ''}">
                    📄 ${t.docs_enabled ? 'Docs' : 'No Docs'}
                </span>
            </div>
            
            <div class="tenant-card-actions">
                <button class="btn btn-ghost" onclick="editTenant('${t.id}')">
                    ⚙️ Configura
                </button>
                <button class="btn btn-ghost" onclick="testChat('${t.id}')">
                    💬 Test
                </button>
            </div>
        </div>
    `).join('');
}

// ==================== Modal Management ====================
function openModal(title = 'Nuovo Tenant', tenantId = null) {
    editingTenantId = tenantId;
    document.getElementById('modal-title').textContent = title;
    document.getElementById('tenant-modal').classList.remove('hidden');

    // Reset form
    document.getElementById('tenant-name').value = '';
    document.getElementById('llm-provider').value = 'openai';
    document.getElementById('llm-api-key').value = '';
    document.getElementById('llm-model-select').value = 'gpt-4o';
    document.getElementById('llm-model-custom').value = '';
    document.getElementById('custom-model-group').classList.add('hidden');
    document.getElementById('db-enabled').checked = false;
    document.getElementById('db-fields').classList.add('hidden');
    pendingFiles = [];
    document.getElementById('file-list').innerHTML = '';

    // Show first tab
    switchTab('basic');
}

function closeModal() {
    document.getElementById('tenant-modal').classList.add('hidden');
    editingTenantId = null;
    pendingFiles = [];
}

function switchTab(tabName) {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelector(`.tab[data-tab="${tabName}"]`).classList.add('active');

    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
    document.getElementById(`tab-${tabName}`).classList.add('active');
}

// ==================== Tenant CRUD ====================
async function saveTenant() {
    const name = document.getElementById('tenant-name').value.trim();
    const provider = document.getElementById('llm-provider').value;
    const apiKey = document.getElementById('llm-api-key').value.trim();

    let model = document.getElementById('llm-model-select').value;
    if (model === 'custom') {
        model = document.getElementById('llm-model-custom').value.trim();
    }

    if (!name) {
        showToast('Inserisci il nome del tenant', 'error');
        return;
    }

    try {
        let tenantId;

        if (editingTenantId) {
            // Update existing
            const llmPayload = {
                provider,
                model_name: model || null
            };

            // Only add API key if user typed something (didn't leave it empty/dots)
            if (apiKey) {
                llmPayload.api_key = apiKey;
            }

            await api(`/tenants/${editingTenantId}/llm`, {
                method: 'PUT',
                body: JSON.stringify(llmPayload)
            });

            tenantId = editingTenantId;
            showToast('Tenant aggiornato!', 'success');
        } else {
            // Create new
            if (!apiKey) {
                showToast('Inserisci la API Key LLM', 'error');
                return;
            }

            const tenant = await api('/tenants', {
                method: 'POST',
                body: JSON.stringify({
                    name,
                    llm_provider: provider,
                    llm_api_key: apiKey,
                    llm_model_name: model || null
                })
            });
            tenantId = tenant.id;
            showToast('Tenant creato!', 'success');
        }

        // Save database config if enabled
        const dbEnabled = document.getElementById('db-enabled').checked;
        if (dbEnabled) {
            await saveDatabaseConfig(tenantId);
        }

        // Upload pending files
        if (pendingFiles.length > 0) {
            await uploadFiles(tenantId);
        }

        closeModal();
        loadTenants();

    } catch (error) {
        showToast(error.message, 'error');
    }
}

async function saveDatabaseConfig(tenantId) {
    const host = document.getElementById('db-host').value.trim();
    const database = document.getElementById('db-name').value.trim();

    if (!host || !database) {
        throw new Error('Host e Nome Database sono obbligatori se la connessione è abilitata');
    }

    const config = {
        enabled: true,
        db_type: document.getElementById('db-type').value,
        host: host,
        port: parseInt(document.getElementById('db-port').value) || 5432,
        database: database,
        username: document.getElementById('db-user').value.trim(),
        schema_name: document.getElementById('db-schema').value,
        password: document.getElementById('db-password').value,
        allowed_tables: document.getElementById('db-tables').value
            .split(',')
            .map(t => t.trim())
            .filter(t => t),
        allowed_columns: {},
        max_rows: 500,
        timeout_seconds: 30
    };

    await api(`/tenants/${tenantId}/database`, {
        method: 'PUT',
        body: JSON.stringify(config)
    });
}

async function testDatabaseConnection() {
    const btn = document.getElementById('test-db-btn');
    const originalText = btn.innerHTML;

    const config = {
        db_type: document.getElementById('db-type').value,
        host: document.getElementById('db-host').value,
        port: parseInt(document.getElementById('db-port').value) || 5432,
        database: document.getElementById('db-name').value,
        username: document.getElementById('db-user').value,
        schema_name: document.getElementById('db-schema').value,
        password: document.getElementById('db-password').value || '',
        allowed_tables: [],
        allowed_columns: {}
    };

    if (!config.host && config.db_type !== 'sqlite') {
        showToast('Inserisci l\'host del database', 'error');
        return;
    }

    try {
        btn.disabled = true;
        btn.innerHTML = '⌛ Test in corso...';

        const url = editingTenantId ? `/tenants/test-db?tenant_id=${editingTenantId}` : '/tenants/test-db';

        // Also include schema in URL if editing? No, it's in the body
        const result = await api(url, {
            method: 'POST',
            body: JSON.stringify(config)
        });

        if (result.status === 'success') {
            showToast(result.message, 'success');
        } else {
            showToast(`Errore: ${result.message}`, 'error');
        }
    } catch (error) {
        showToast(error.message, 'error');
    } finally {
        btn.disabled = false;
        btn.innerHTML = originalText;
    }
}

async function uploadFiles(tenantId) {
    for (const file of pendingFiles) {
        const formData = new FormData();
        formData.append('file', file);
        formData.append('trigger_indexing', 'true');

        await fetch(`${API_BASE}/tenants/${tenantId}/documents`, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${authToken}`
            },
            body: formData
        });
    }
    showToast(`${pendingFiles.length} file caricati!`, 'success');
}

function editTenant(tenantId) {
    const tenant = tenants.find(t => t.id === tenantId);
    if (!tenant) return;

    openModal('Configura Tenant', tenantId);
    document.getElementById('tenant-name').value = tenant.name;
    document.getElementById('llm-provider').value = tenant.llm_provider || 'openai';

    const modelSelect = document.getElementById('llm-model-select');
    const modelCustom = document.getElementById('llm-model-custom');
    const customGroup = document.getElementById('custom-model-group');

    if (tenant.llm_model) {
        // Check if model exists in dropdown
        const exists = Array.from(modelSelect.options).some(opt => opt.value === tenant.llm_model);
        if (exists) {
            modelSelect.value = tenant.llm_model;
            customGroup.classList.add('hidden');
        } else {
            modelSelect.value = 'custom';
            modelCustom.value = tenant.llm_model;
            customGroup.classList.remove('hidden');
        }
    } else {
        modelSelect.value = 'gpt-4o';
        customGroup.classList.add('hidden');
    }

    // Show dots if API Key is already set
    if (tenant.has_llm_key) {
        document.getElementById('llm-api-key').placeholder = '••••••••••••••••';
    } else {
        document.getElementById('llm-api-key').placeholder = 'sk-...';
    }

    // Database fields
    if (tenant.db_enabled) {
        document.getElementById('db-enabled').checked = true;
        document.getElementById('db-fields').classList.remove('hidden');
        document.getElementById('db-type').value = tenant.db_type || 'postgres';
        document.getElementById('db-host').value = tenant.db_host || '';
        document.getElementById('db-port').value = tenant.db_port || 5432;

        // Robust population for database name
        const dbNameVal = tenant.db_name || tenant.db_database || '';
        document.getElementById('db-name').value = dbNameVal;

        document.getElementById('db-user').value = tenant.db_user || '';

        const schemaSelect = document.getElementById('db-schema');
        schemaSelect.innerHTML = `<option value="${tenant.db_schema || 'public'}">${tenant.db_schema || 'public'}</option>`;
        schemaSelect.value = tenant.db_schema || 'public';

        document.getElementById('db-tables').value = (tenant.db_allowed_tables || []).join(', ');

        if (tenant.has_db_password) {
            document.getElementById('db-password').placeholder = '••••••••';
        }
    }
}

// ==================== Chat Test Management ====================
let chatActiveTenantId = null;
let chatSessionId = null;

function openChatModal(tenantId) {
    const tenant = tenants.find(t => t.id === tenantId);
    if (!tenant) return;

    chatActiveTenantId = tenantId;
    // Generate a fresh session ID for every new chat opening to ensure a clean state
    chatSessionId = (typeof crypto !== 'undefined' && crypto.randomUUID)
        ? crypto.randomUUID()
        : `sess_${Date.now()}_${Math.random().toString(16).slice(2)}`;

    document.getElementById('chat-modal-title').textContent = `Test Chat: ${tenant.name}`;
    document.getElementById('chat-modal').classList.remove('hidden');

    // Clear history except first message
    const history = document.getElementById('chat-history');
    history.innerHTML = `
        <div class="chat-message system">
            <p>Connesso a <strong>${tenant.name}</strong>. Il server cercherà automaticamente tra database e documenti.</p>
        </div>
    `;

    document.getElementById('chat-input').focus();
}

function closeChatModal() {
    document.getElementById('chat-modal').classList.add('hidden');
    chatActiveTenantId = null;
}

function addChatMessage(content, type) {
    const history = document.getElementById('chat-history');
    const msg = document.createElement('div');
    msg.className = `chat-message ${type}`;
    msg.innerHTML = `<p>${content}</p>`;
    history.appendChild(msg);
    history.scrollTop = history.scrollHeight;
    return msg;
}

async function sendChatMessage() {
    const input = document.getElementById('chat-input');
    const btn = document.querySelector('#chat-form button');
    const isStream = document.getElementById('chat-stream-toggle').checked;
    const query = input.value.trim();
    if (!query || !chatActiveTenantId) return;

    input.value = '';
    btn.disabled = true;
    addChatMessage(query, 'user');

    const loadingMsg = addChatMessage(isStream ? '' : '⌨️ L\'AI sta elaborando...', 'ai');
    let fullContent = "";

    try {
        if (isStream) {
            const response = await fetch(`${API_BASE}/tenants/${chatActiveTenantId}/chat`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${authToken}`
                },
                body: JSON.stringify({
                    query,
                    session_id: chatSessionId,
                    site_id: "1",
                    stream: true
                })
            });

            if (!response.ok) throw new Error('Streaming request failed');

            const reader = response.body.getReader();
            const decoder = new TextDecoder();

            while (true) {
                const { done, value } = await reader.read();
                if (done) break;

                const chunk = decoder.decode(value, { stream: true });
                fullContent += chunk;

                // Authoritative Bypass Check: if we see the bypass signal, we might want to clear and show only that
                // However, the backend is now designed to yield the full bypass content.
                // We'll just keep appending/updating.
                loadingMsg.querySelector('p').textContent = fullContent.replace('[[DIRECT_DISPLAY]]', '');
                document.getElementById('chat-history').scrollTop = document.getElementById('chat-history').scrollHeight;
            }
        } else {
            const response = await api(`/tenants/${chatActiveTenantId}/chat`, {
                method: 'POST',
                body: JSON.stringify({
                    query,
                    session_id: chatSessionId,
                    site_id: "1"
                })
            });
            loadingMsg.innerHTML = `<p>${response.answer}</p>`;
            if (response.query_type) {
                const info = document.createElement('div');
                info.className = 'chat-message system';
                info.innerHTML = `<p><small>Sorgente: ${response.query_type}</small></p>`;
                document.getElementById('chat-history').appendChild(info);
            }
        }
    } catch (error) {
        if (!fullContent) loadingMsg.remove();
        addChatMessage(`Errore: ${error.message}`, 'error');
    } finally {
        btn.disabled = false;
        input.focus();
    }
}

async function testChat(tenantId) {
    openChatModal(tenantId);
}

// ==================== File Upload ====================
function setupFileUpload() {
    const uploadArea = document.getElementById('upload-area');
    const fileInput = document.getElementById('file-input');

    uploadArea.addEventListener('click', () => fileInput.click());

    uploadArea.addEventListener('dragover', (e) => {
        e.preventDefault();
        uploadArea.classList.add('dragover');
    });

    uploadArea.addEventListener('dragleave', () => {
        uploadArea.classList.remove('dragover');
    });

    uploadArea.addEventListener('drop', (e) => {
        e.preventDefault();
        uploadArea.classList.remove('dragover');
        handleFiles(e.dataTransfer.files);
    });

    fileInput.addEventListener('change', (e) => {
        handleFiles(e.target.files);
    });
}

function handleFiles(files) {
    for (const file of files) {
        const ext = file.name.split('.').pop().toLowerCase();
        if (['pdf', 'txt', 'md', 'docx'].includes(ext)) {
            pendingFiles.push(file);
        } else {
            showToast(`Formato non supportato: ${file.name}`, 'error');
        }
    }
    renderFileList();
}

function renderFileList() {
    const container = document.getElementById('file-list');
    container.innerHTML = pendingFiles.map((f, i) => `
        <div class="file-item">
            <span class="file-item-name">
                <span>📄</span>
                <span>${f.name}</span>
            </span>
            <button class="file-remove" onclick="removeFile(${i})">×</button>
        </div>
    `).join('');
}

function removeFile(index) {
    pendingFiles.splice(index, 1);
    renderFileList();
}

// ==================== Event Listeners ====================
document.addEventListener('DOMContentLoaded', () => {
    // Login form
    document.getElementById('login-form').addEventListener('submit', async (e) => {
        e.preventDefault();
        const email = document.getElementById('email').value;
        const password = document.getElementById('password').value;
        const errorEl = document.getElementById('login-error');
        const btn = e.target.querySelector('button');

        btn.disabled = true;
        errorEl.classList.add('hidden');

        try {
            await login(email, password);
        } catch (error) {
            errorEl.textContent = error.message;
            errorEl.classList.remove('hidden');
        } finally {
            btn.disabled = false;
        }
    });

    // Navigation
    document.querySelectorAll('.nav-item').forEach(item => {
        item.addEventListener('click', (e) => {
            e.preventDefault();
            showSection(item.dataset.section);
        });
    });

    // Logout
    document.getElementById('logout-btn').addEventListener('click', logout);

    // New tenant button
    document.getElementById('new-tenant-btn').addEventListener('click', () => openModal());

    // Modal close
    document.querySelector('.modal-close').addEventListener('click', closeModal);
    document.querySelectorAll('.modal-backdrop').forEach(b => b.addEventListener('click', () => {
        closeModal();
        closeChatModal();
    }));
    document.querySelector('.modal-cancel').addEventListener('click', closeModal);

    // Chat Modal close
    document.querySelector('.chat-modal-close').addEventListener('click', closeChatModal);

    // Chat Form
    document.getElementById('chat-form').addEventListener('submit', (e) => {
        e.preventDefault();
        sendChatMessage();
    });

    // Modal tabs
    document.querySelectorAll('.tab').forEach(tab => {
        tab.addEventListener('click', () => switchTab(tab.dataset.tab));
    });

    // Save tenant
    document.getElementById('save-tenant-btn').addEventListener('click', saveTenant);

    // Database toggle
    document.getElementById('db-enabled').addEventListener('change', (e) => {
        document.getElementById('db-fields').classList.toggle('hidden', !e.target.checked);
    });

    // Model select toggle
    document.getElementById('llm-model-select').addEventListener('change', (e) => {
        document.getElementById('custom-model-group').classList.toggle('hidden', e.target.value !== 'custom');
    });

    // Fetch schemas
    document.getElementById('fetch-schemas-btn').addEventListener('click', async (e) => {
        e.preventDefault();
        const btn = e.target;
        const originalText = btn.innerHTML;

        const config = {
            db_type: document.getElementById('db-type').value,
            host: document.getElementById('db-host').value,
            port: parseInt(document.getElementById('db-port').value) || 5432,
            database: document.getElementById('db-name').value,
            username: document.getElementById('db-user').value,
            password: document.getElementById('db-password').value || '',
        };

        try {
            btn.disabled = true;
            btn.innerHTML = '⌛...';

            const url = editingTenantId ? `/tenants/fetch-schemas?tenant_id=${editingTenantId}` : '/tenants/fetch-schemas';
            const result = await api(url, {
                method: 'POST',
                body: JSON.stringify(config)
            });

            const select = document.getElementById('db-schema');
            const currentVal = select.value;
            select.innerHTML = '';

            result.schemas.forEach(s => {
                const opt = document.createElement('option');
                opt.value = s;
                opt.textContent = s;
                select.appendChild(opt);
            });

            if (result.schemas.includes(currentVal)) {
                select.value = currentVal;
            }
            showToast('Schemi aggiornati!', 'success');
        } catch (error) {
            showToast(`Errore: ${error.message}`, 'error');
        } finally {
            btn.disabled = false;
            btn.innerHTML = originalText;
        }
    });

    // Test DB connection
    document.getElementById('test-db-btn').addEventListener('click', (e) => {
        e.preventDefault();
        testDatabaseConnection();
    });

    // File upload
    setupFileUpload();

    // Check auth on load
    checkAuth();
});
