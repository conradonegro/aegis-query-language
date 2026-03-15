const STORAGE_KEY = 'aegis_api_key';

function getModal() {
    return document.getElementById('auth_modal');
}

function showModal() {
    const modal = getModal();
    if (modal && !modal.open) modal.showModal();
}

function hideModal() {
    const modal = getModal();
    if (modal && modal.open) modal.close();
}

window.addEventListener('aegis:auth-required', showModal);

document.addEventListener('DOMContentLoaded', () => {
    const modal = getModal();
    if (!modal) return;

    const form = modal.querySelector('#auth_form');
    const input = modal.querySelector('#auth_key_input');
    const errorEl = modal.querySelector('#auth_error');
    const btn = modal.querySelector('#auth_submit_btn');

    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        const key = input.value.trim();
        if (!key) return;

        btn.disabled = true;
        errorEl.textContent = '';

        try {
            const res = await fetch('/api/v1/metadata/versions', {
                headers: { 'Authorization': `Bearer ${key}` }
            });

            if (res.ok) {
                localStorage.setItem(STORAGE_KEY, key);
                hideModal();
                window.location.reload();
            } else if (res.status === 401 || res.status === 403) {
                errorEl.textContent = 'Invalid API key. Please try again.';
                input.focus();
            } else {
                errorEl.textContent = `Unexpected server error (${res.status}).`;
            }
        } catch {
            errorEl.textContent = 'Network error. Check your connection.';
        } finally {
            btn.disabled = false;
        }
    });

    // Prompt immediately if no key is stored
    if (!localStorage.getItem(STORAGE_KEY)) {
        showModal();
    }
});
