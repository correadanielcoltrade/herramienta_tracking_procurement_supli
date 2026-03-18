document.addEventListener("DOMContentLoaded", () => {
    const form = document.getElementById("loginForm");
    const message = document.getElementById("loginMessage");

    if (!form) {
        return;
    }

    form.addEventListener("submit", async (event) => {
        event.preventDefault();
        message.textContent = "";

        const formData = new FormData(form);
        const payload = {
            username: formData.get("username"),
            password: formData.get("password"),
        };

        const response = await fetch("/auth/login", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
        });

        if (!response.ok) {
            const data = await response.json().catch(() => ({}));
            message.textContent = data.error || "Error de autenticacion";
            return;
        }

        const data = await response.json();
        if (data.token) {
            localStorage.setItem("access_token", data.token);
        }
        const role = data.user?.role || "USER";
        if (role === "ADMIN") {
            window.location.href = "/admin/embarques";
        } else {
            window.location.href = "/dashboard";
        }
    });
});
