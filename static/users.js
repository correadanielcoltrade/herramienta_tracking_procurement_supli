document.addEventListener("DOMContentLoaded", () => {
    const usersBody = document.getElementById("usersBody");
    const userForm = document.getElementById("userForm");
    const userFormTitle = document.getElementById("userFormTitle");
    const resetUserForm = document.getElementById("resetUserForm");
    const message = document.getElementById("userFormMessage");

    let editingUsername = null;

    function resetForm() {
        editingUsername = null;
        userForm.reset();
        userFormTitle.textContent = "Nuevo usuario";
        userForm.elements.username.disabled = false;
        message.textContent = "";
    }

    async function loadUsers() {
        const response = await window.App.authFetch("/admin/api/users");
        const data = await response.json();
        renderUsers(data.data || []);
    }

    function renderUsers(users) {
        usersBody.innerHTML = "";
        users.forEach((user) => {
            const row = document.createElement("tr");
            row.innerHTML = `
                <td>${user.username}</td>
                <td>${user.name || ""}</td>
                <td>${user.role || ""}</td>
                <td>
                    <button class="ghost-btn small edit-user" data-username="${user.username}">
                        <i class="fas fa-pen"></i>
                    </button>
                    <button class="danger-btn small delete-user" data-username="${user.username}">
                        <i class="fas fa-trash"></i>
                    </button>
                </td>
            `;
            usersBody.appendChild(row);
        });

        usersBody.querySelectorAll(".edit-user").forEach((btn) => {
            btn.addEventListener("click", () => {
                const username = btn.getAttribute("data-username");
                const user = Array.from(users).find((u) => u.username === username);
                if (!user) return;
                editingUsername = username;
                userFormTitle.textContent = `Editar ${username}`;
                userForm.elements.username.value = user.username;
                userForm.elements.username.disabled = true;
                userForm.elements.name.value = user.name || "";
                userForm.elements.role.value = user.role || "USER";
                userForm.elements.password.value = "";
                message.textContent = "";
            });
        });

        usersBody.querySelectorAll(".delete-user").forEach((btn) => {
            btn.addEventListener("click", async () => {
                const username = btn.getAttribute("data-username");
                if (!confirm(`Eliminar usuario ${username}?`)) return;
                const response = await window.App.authFetch(`/admin/api/users/${username}`, {
                    method: "DELETE",
                });
                const data = await response.json().catch(() => ({}));
                if (!response.ok) {
                    message.textContent = data.error || "No se pudo eliminar";
                    return;
                }
                await loadUsers();
                resetForm();
            });
        });
    }

    userForm.addEventListener("submit", async (event) => {
        event.preventDefault();
        message.textContent = "";
        const payload = {
            username: userForm.elements.username.value.trim(),
            name: userForm.elements.name.value.trim(),
            role: userForm.elements.role.value,
            password: userForm.elements.password.value.trim(),
        };

        if (editingUsername) {
            if (!payload.password) {
                delete payload.password;
            }
            const response = await window.App.authFetch(`/admin/api/users/${editingUsername}`, {
                method: "PUT",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload),
            });
            const data = await response.json().catch(() => ({}));
            if (!response.ok) {
                message.textContent = data.error || "No se pudo actualizar";
                return;
            }
        } else {
            const response = await window.App.authFetch("/admin/api/users", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload),
            });
            const data = await response.json().catch(() => ({}));
            if (!response.ok) {
                message.textContent = data.error || "No se pudo crear";
                return;
            }
        }

        await loadUsers();
        resetForm();
    });

    resetUserForm.addEventListener("click", () => resetForm());

    resetForm();
    loadUsers();
});
