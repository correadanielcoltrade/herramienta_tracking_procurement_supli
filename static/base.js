document.addEventListener("DOMContentLoaded", () => {
    const sidebar = document.getElementById("sidebar");
    const sidebarToggles = document.querySelectorAll(".sidebar-toggle");
    const themeToggle = document.getElementById("themeToggle");
    const SIDEBAR_STATE_KEY = "sidebar_state";

    if (sidebarToggles.length && sidebar) {
        const storedSidebarState = localStorage.getItem(SIDEBAR_STATE_KEY) || "expanded";
        if (storedSidebarState === "collapsed" && !window.matchMedia("(max-width: 980px)").matches) {
            document.body.classList.add("sidebar-hidden");
        }
        document.documentElement.classList.remove("sidebar-collapsed");
        window.setTimeout(() => {
            document.documentElement.classList.remove("no-transition");
        }, 0);

        sidebarToggles.forEach((toggle) => {
            toggle.addEventListener("click", () => {
                const isMobile = window.matchMedia("(max-width: 980px)").matches;
                if (isMobile) {
                    sidebar.classList.toggle("open");
                } else {
                    document.body.classList.toggle("sidebar-hidden");
                    const collapsed = document.body.classList.contains("sidebar-hidden");
                    localStorage.setItem(SIDEBAR_STATE_KEY, collapsed ? "collapsed" : "expanded");
                }
            });
        });
    }
    window.addEventListener("resize", () => {
        if (window.matchMedia("(max-width: 980px)").matches) {
            document.body.classList.remove("sidebar-hidden");
        } else {
            sidebar?.classList.remove("open");
            const storedSidebarState = localStorage.getItem(SIDEBAR_STATE_KEY) || "expanded";
            if (storedSidebarState === "collapsed") {
                document.body.classList.add("sidebar-hidden");
            } else {
                document.body.classList.remove("sidebar-hidden");
            }
        }
    });

    const currentPath = window.location.pathname;
    document.querySelectorAll(".menu-item").forEach((item) => {
        if (item.getAttribute("href") === currentPath) {
            item.classList.add("active");
        }
    });

    const storedTheme = localStorage.getItem("theme") || "light";
    setTheme(storedTheme);

    if (themeToggle) {
        themeToggle.addEventListener("click", () => {
            const next = document.body.getAttribute("data-theme") === "dark" ? "light" : "dark";
            setTheme(next);
        });
    }

    const logoutBtn = document.querySelector(".logout-btn");
    if (logoutBtn) {
        logoutBtn.addEventListener("click", () => {
            localStorage.removeItem("access_token");
        });
    }
});

function setTheme(theme) {
    document.documentElement.setAttribute("data-theme", theme);
    document.body.setAttribute("data-theme", theme);
    localStorage.setItem("theme", theme);
    const icon = document.querySelector("#themeToggle i");
    if (icon) {
        icon.className = theme === "dark" ? "fas fa-sun" : "fas fa-moon";
    }
}

function getCookie(name) {
    const value = `; ${document.cookie}`;
    const parts = value.split(`; ${name}=`);
    if (parts.length === 2) {
        return parts.pop().split(";").shift();
    }
    return "";
}

function getToken() {
    return localStorage.getItem("access_token") || getCookie("access_token");
}

async function authFetch(url, options = {}) {
    const token = getToken();
    const headers = options.headers || {};
    if (token) {
        headers["Authorization"] = `Bearer ${token}`;
    }
    return fetch(url, { ...options, headers });
}

window.App = {
    getToken,
    authFetch,
    setTheme,
};
