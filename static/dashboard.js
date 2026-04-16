document.addEventListener("DOMContentLoaded", () => {
    const shipmentsGrid = document.getElementById("shipmentsGrid");
    const clearFiltersBtn = document.getElementById("clearFilters");
    const exportFilteredBtn = document.getElementById("exportFilteredBtn");

    const filters = {
        imp: document.getElementById("filterImp"),
        proveedor: document.getElementById("filterProveedor"),
        estado: document.getElementById("filterEstado"),
        canal: document.getElementById("filterCanal"),
        tipoCompra: document.getElementById("filterTipoCompra"),
        producto: document.getElementById("filterProducto"),
        marca: document.getElementById("filterMarca"),
        sku: document.getElementById("filterSku"),
        fechaDesde: document.getElementById("filterFechaDesde"),
        fechaHasta: document.getElementById("filterFechaHasta"),
    };

    let shipments = [];
    let activeId = null;
    const shipmentDetails = new Map();
    let filterTimer = null;
    const CHANNELS = ["retail", "resellers", "corporativo", "ecommerce", "telcom", "libre"];

    function formatCOP(value) {
        return new Intl.NumberFormat("es-CO", {
            style: "currency",
            currency: "COP",
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
        }).format(value);
    }

    async function loadSummary() {
        const params = new URLSearchParams();
        if (filters.imp?.value.trim()) params.set("imp", filters.imp.value.trim());
        if (filters.proveedor?.value.trim()) params.set("proveedor", filters.proveedor.value.trim());
        if (filters.estado?.value.trim()) params.set("estado", filters.estado.value.trim());
        if (filters.canal?.value.trim()) params.set("canal", filters.canal.value.trim());
        if (filters.tipoCompra?.value.trim()) params.set("tipo_compra", filters.tipoCompra.value.trim());
        if (filters.producto?.value.trim()) params.set("producto", filters.producto.value.trim());
        if (filters.marca?.value.trim()) params.set("marca", filters.marca.value.trim());
        if (filters.sku?.value.trim()) params.set("sku", filters.sku.value.trim());
        if (filters.fechaDesde?.value) params.set("fecha_desde", filters.fechaDesde.value);
        if (filters.fechaHasta?.value) params.set("fecha_hasta", filters.fechaHasta.value);
        const query = params.toString();
        const response = await window.App.authFetch(
            `/api/shipments-summary${query ? `?${query}` : ""}`
        );
        const data = await response.json();
        shipments = data.data || [];
        renderCards(shipments);
        if (!shipments.some((item) => item.id === activeId)) {
            activeId = null;
        }
    }

    function normalizeStatus(status) {
        return (status || "").trim().toUpperCase();
    }

    function statusMeta(status) {
        const normalized = normalizeStatus(status);
        const map = {
            ENTREGADO: { label: "Entregado", icon: "fa-circle-check", tone: "status-success" },
            "EN NACIONALIZACION": { label: "En Nacionalizacion", icon: "fa-building-flag", tone: "status-info" },
            ABIERTO: { label: "Abierto", icon: "fa-box-open", tone: "status-warning" },
            "EN TRANSITO": { label: "En transito", icon: "fa-truck-moving", tone: "status-info" },
            "EN INCLUSION": { label: "En inclusion", icon: "fa-list-check", tone: "status-warning" },
            "EN DEPOSITO": { label: "En deposito", icon: "fa-warehouse", tone: "status-neutral" },
            "EN PROCESO": { label: "En proceso", icon: "fa-gears", tone: "status-info" },
            PENDIENTE: { label: "Pendiente", icon: "fa-hourglass-half", tone: "status-neutral" },
        };
        return (
            map[normalized] || {
                label: status || "Pendiente",
                icon: "fa-circle-question",
                tone: "status-neutral",
            }
        );
    }

    function buildProductsTableHTML(products, selectedChannel = "") {
        if (!products.length) {
            return `<div class="detail-empty">Sin productos</div>`;
        }
        const totals = {
            q_total: 0,
            costo_proyectado_ddp: 0,
            retail: 0,
            resellers: 0,
            corporativo: 0,
            ecommerce: 0,
            telcom: 0,
            libre: 0,
        };

        const rowsHtml = products
            .map((product) => {
                totals.q_total += Number(product.q_total || 0);
                totals.costo_proyectado_ddp += Number(product.costo_proyectado_ddp || 0);
                CHANNELS.forEach((channel) => {
                    totals[channel] += Number(product[channel] || 0);
                });
                return `
                    <tr>
                        <td>${product.producto || ""}</td>
                        <td>${product.marca || ""}</td>
                        <td>${product.sku || ""}</td>
                        <td>${product.q_total || 0}</td>
                        <td>${formatCOP(Number(product.costo_proyectado_ddp || 0))}</td>
                        ${selectedChannel ? `<td>${product[selectedChannel] || 0}</td>` : `
                        <td>${product.retail || 0}</td>
                        <td>${product.resellers || 0}</td>
                        <td>${product.corporativo || 0}</td>
                        <td>${product.ecommerce || 0}</td>
                        <td>${product.telcom || 0}</td>
                        <td>${product.libre || 0}</td>
                        `}
                    </tr>
                `;
            })
            .join("");

        const channelHeader = selectedChannel
            ? selectedChannel.charAt(0).toUpperCase() + selectedChannel.slice(1)
            : "";

        return `
            <div class="table-wrap">
                <table class="mini-table">
                    <thead>
                        <tr>
                            <th>Producto</th>
                            <th>Marca</th>
                            <th>SKU</th>
                            <th>Q Total</th>
                            <th>Costo Proyectado</th>
                            ${
                                selectedChannel
                                    ? `<th>${channelHeader}</th>`
                                    : `
                            <th>Retail</th>
                            <th>Resellers</th>
                            <th>Corporativo</th>
                            <th>Ecommerce</th>
                            <th>Telcom</th>
                            <th>Libre</th>`
                            }
                        </tr>
                    </thead>
                    <tbody>
                        ${rowsHtml}
                    </tbody>
                    <tfoot>
                        <tr>
                            <td colspan="3">Totales</td>
                            <td>${totals.q_total}</td>
                            <td>${formatCOP(Number(totals.costo_proyectado_ddp || 0))}</td>
                            ${
                                selectedChannel
                                    ? `<td>${totals[selectedChannel] || 0}</td>`
                                    : `
                            <td>${totals.retail}</td>
                            <td>${totals.resellers}</td>
                            <td>${totals.corporativo}</td>
                            <td>${totals.ecommerce}</td>
                            <td>${totals.telcom}</td>
                            <td>${totals.libre}</td>`
                            }
                        </tr>
                    </tfoot>
                </table>
            </div>
        `;
    }

    function renderCards(items) {
        shipmentsGrid.innerHTML = "";
        if (!items.length) {
            shipmentsGrid.innerHTML = `<div class="detail-empty">No hay embarques con esos filtros.</div>`;
            return;
        }
        items.forEach((item) => {
            const meta = statusMeta(item.estado_imp);
            const isDelivered = normalizeStatus(item.estado_imp) === "ENTREGADO" && !!item.fecha_llegada;
            const card = document.createElement("div");
            card.className = `shipment-card${item.id === activeId ? " open active" : ""}${
                isDelivered ? " delivered" : ""
            }`;
            card.setAttribute("role", "button");
            card.setAttribute("tabindex", "0");
            const selectedChannel = filters.canal?.value.trim().toLowerCase() || "";
            const cachedDetail = shipmentDetails.get(item.id);
            const detailHtml = cachedDetail
                ? buildProductsTableHTML(cachedDetail.productos || [], selectedChannel)
                : `<div class="detail-empty">Abre para cargar productos.</div>`;
            card.innerHTML = `
                <div class="card-row">
                    <div class="card-imp">${item.imp || "Sin IMP"}</div>
                    <div class="card-right">
                        <span class="status-pill ${meta.tone}">
                            <i class="fas ${meta.icon}"></i> ${meta.label}
                        </span>
                        <i class="fas fa-chevron-down card-chevron"></i>
                    </div>
                </div>
                <div class="card-row">
                    <div class="card-meta">${item.proveedor || "Sin proveedor"}</div>
                    <div class="card-badge">${item.total_qty || 0} uds</div>
                </div>
                <div class="card-row">
                    <div class="card-meta">${item.tipo_compra || "Sin tipo compra"}</div>
                    <div class="card-meta">${item.fecha_llegada || ""}</div>
                </div>
                <div class="card-detail" data-detail-id="${item.id}">
                    ${detailHtml}
                </div>
            `;
            card.addEventListener("click", () => toggleCard(item, card));
            card.addEventListener("keydown", (event) => {
                if (event.key === "Enter" || event.key === " ") {
                    event.preventDefault();
                    toggleCard(item, card);
                }
            });
            shipmentsGrid.appendChild(card);
        });
    }

    async function toggleCard(shipment, card) {
        const isOpen = card.classList.contains("open");
        document.querySelectorAll(".shipment-card").forEach((c) => c.classList.remove("open", "active"));
        if (!isOpen) {
            activeId = shipment.id;
            card.classList.add("open", "active");
            const detailContainer = card.querySelector(`[data-detail-id="${shipment.id}"]`);
            if (detailContainer) {
                const cached = shipmentDetails.get(shipment.id);
                const selectedChannel = filters.canal?.value.trim().toLowerCase() || "";
                if (cached) {
                    detailContainer.innerHTML = buildProductsTableHTML(
                        cached.productos || [],
                        selectedChannel
                    );
                } else {
                    detailContainer.innerHTML = `<div class="detail-empty">Cargando productos...</div>`;
                    const detail = await loadShipmentDetail(shipment.id);
                    detailContainer.innerHTML = buildProductsTableHTML(
                        detail?.productos || [],
                        selectedChannel
                    );
                }
            }
        } else {
            activeId = null;
        }
    }

    Object.values(filters).forEach((input) => {
        if (!input) return;
        input.addEventListener("input", scheduleLoad);
        input.addEventListener("change", scheduleLoad);
    });

    if (clearFiltersBtn) {
        clearFiltersBtn.addEventListener("click", () => {
            Object.values(filters).forEach((input) => {
                if (input) input.value = "";
            });
            loadSummary();
        });
    }

    if (exportFilteredBtn) {
        exportFilteredBtn.addEventListener("click", () => {
            const params = new URLSearchParams();
            if (filters.imp?.value.trim()) params.set("imp", filters.imp.value.trim());
            if (filters.proveedor?.value.trim()) params.set("proveedor", filters.proveedor.value.trim());
            if (filters.estado?.value.trim()) params.set("estado", filters.estado.value.trim());
            if (filters.canal?.value.trim()) params.set("canal", filters.canal.value.trim());
            if (filters.tipoCompra?.value.trim()) params.set("tipo_compra", filters.tipoCompra.value.trim());
            if (filters.producto?.value.trim()) params.set("producto", filters.producto.value.trim());
            if (filters.marca?.value.trim()) params.set("marca", filters.marca.value.trim());
            if (filters.sku?.value.trim()) params.set("sku", filters.sku.value.trim());
            if (filters.fechaDesde?.value) params.set("fecha_desde", filters.fechaDesde.value);
            if (filters.fechaHasta?.value) params.set("fecha_hasta", filters.fechaHasta.value);
            const query = params.toString();
            window.location.href = `/api/export-excel-filtered${query ? `?${query}` : ""}`;
        });
    }

    function scheduleLoad() {
        if (filterTimer) {
            clearTimeout(filterTimer);
        }
        filterTimer = setTimeout(() => {
            loadSummary();
        }, 250);
    }

    async function loadShipmentDetail(id) {
        if (shipmentDetails.has(id)) {
            return shipmentDetails.get(id);
        }
        const response = await window.App.authFetch(`/api/shipments/${id}`);
        const data = await response.json();
        const detail = data.data?.shipment || null;
        if (detail) {
            shipmentDetails.set(id, detail);
        }
        return detail;
    }

    loadSummary();
});
