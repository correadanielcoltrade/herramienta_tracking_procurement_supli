document.addEventListener("DOMContentLoaded", () => {
    const shipmentsBody = document.getElementById("shipmentsBody");
    const productsBody = document.getElementById("productsBody");
    const addProductRowBtn = document.getElementById("addProductRow");
    const shipmentForm = document.getElementById("shipmentForm");
    const formTitle = document.getElementById("formTitle");
    const resetFormBtn = document.getElementById("resetFormBtn");
    const selectAll = document.getElementById("selectAll");
    const bulkDeleteBtn = document.getElementById("bulkDeleteBtn");
    const exportExcelBtn = document.getElementById("exportExcelBtn");
    const exportJsonBtn = document.getElementById("exportJsonBtn");
    const templateBtn = document.getElementById("templateBtn");
    const importForm = document.getElementById("importForm");
    const importStatus = document.getElementById("importStatus");
    const newShipmentBtn = document.getElementById("newShipmentBtn");
    const shipmentModal = document.getElementById("shipmentModal");
    const closeShipmentModalBtn = document.getElementById("closeShipmentModal");

    let shipments = [];
    let editingId = null;
    const shipmentDetails = new Map();
    let filterTimer = null;

    const filters = {
        imp: document.getElementById("filterImp"),
        proveedor: document.getElementById("filterProveedor"),
        producto: document.getElementById("filterProducto"),
        marca: document.getElementById("filterMarca"),
        sku: document.getElementById("filterSku"),
        fecha: document.getElementById("filterFecha"),
        estado: document.getElementById("filterEstado"),
        tipoCompra: document.getElementById("filterTipoCompra"),
    };

    const CHANNELS = ["retail", "resellers", "corporativo", "ecommerce", "telcom", "libre"];
    const STATUS_CUSTOM_ATTR = "data-custom-status";

    function normalizeStatus(status) {
        return (status || "").trim().toUpperCase();
    }

    function clearCustomStatusOption(selectEl) {
        if (!selectEl) return;
        const customOption = selectEl.querySelector(`option[${STATUS_CUSTOM_ATTR}]`);
        if (customOption) {
            customOption.remove();
        }
    }

    function setStatusSelectValue(selectEl, rawValue) {
        if (!selectEl) return;
        clearCustomStatusOption(selectEl);
        const normalized = normalizeStatus(rawValue);
        const optionValues = Array.from(selectEl.options).map((opt) => opt.value);
        if (normalized && optionValues.includes(normalized)) {
            selectEl.value = normalized;
            return;
        }
        if (rawValue) {
            const customOption = document.createElement("option");
            customOption.value = rawValue;
            customOption.textContent = `⚠️ ${rawValue}`;
            customOption.setAttribute(STATUS_CUSTOM_ATTR, "1");
            selectEl.appendChild(customOption);
            selectEl.value = rawValue;
            return;
        }
        selectEl.value = "";
    }

    function createProductRow(product = {}) {
        const row = document.createElement("tr");
        row.innerHTML = `
            <td><input class="table-input" data-field="producto" value="${product.producto || ""}"></td>
            <td><input class="table-input" data-field="marca" value="${product.marca || ""}"></td>
            <td><input class="table-input" data-field="upc" value="${product.upc || ""}"></td>
            <td><input class="table-input" data-field="sku" value="${product.sku || ""}"></td>
            <td><input class="table-input" data-field="q_total" type="number" value="${product.q_total ?? 0}"></td>
            <td><input class="table-input" data-field="costo_fob_usd" type="number" step="0.01" value="${product.costo_fob_usd ?? 0}"></td>
            <td><input class="table-input" data-field="costo_proyectado_ddp" type="number" step="0.01" value="${product.costo_proyectado_ddp ?? 0}"></td>
            <td><input class="table-input" data-field="retail" type="number" value="${product.retail ?? 0}"></td>
            <td><input class="table-input" data-field="resellers" type="number" value="${product.resellers ?? 0}"></td>
            <td><input class="table-input" data-field="corporativo" type="number" value="${product.corporativo ?? 0}"></td>
            <td><input class="table-input" data-field="ecommerce" type="number" value="${product.ecommerce ?? 0}"></td>
            <td><input class="table-input" data-field="telcom" type="number" value="${product.telcom ?? 0}"></td>
            <td><input class="table-input" data-field="libre" type="number" value="${product.libre ?? 0}"></td>
            <td><input class="table-input" data-field="confirmacion_cantidades_recibidas" value="${product.confirmacion_cantidades_recibidas || ""}"></td>
            <td><input class="table-input" data-field="observaciones" value="${product.observaciones || ""}"></td>
            <td><button class="icon-btn small remove-row" type="button"><i class="fas fa-times"></i></button></td>
        `;
        row.querySelector(".remove-row").addEventListener("click", () => {
            row.remove();
        });
        return row;
    }

    function resetForm() {
        editingId = null;
        formTitle.textContent = "Nuevo embarque";
        shipmentForm.reset();
        clearCustomStatusOption(shipmentForm.elements.estado_imp);
        productsBody.innerHTML = "";
        productsBody.appendChild(createProductRow());
    }

    function openModal() {
        if (!shipmentModal) return;
        shipmentModal.classList.add("open");
        shipmentModal.setAttribute("aria-hidden", "false");
        document.body.classList.add("modal-open");
    }

    function closeModal() {
        if (!shipmentModal) return;
        shipmentModal.classList.remove("open");
        shipmentModal.setAttribute("aria-hidden", "true");
        document.body.classList.remove("modal-open");
    }

    function collectProducts() {
        const rows = Array.from(productsBody.querySelectorAll("tr"));
        return rows.map((row) => {
            const product = {};
            row.querySelectorAll("[data-field]").forEach((input) => {
                const field = input.getAttribute("data-field");
                let value = input.value;
                if (["q_total", "costo_fob_usd", "costo_proyectado_ddp", "retail", "resellers", "corporativo", "ecommerce", "telcom", "libre"].includes(field)) {
                    value = Number(value || 0);
                }
                product[field] = value;
            });
            return product;
        });
    }

    function applyFilters() {
        if (filterTimer) {
            clearTimeout(filterTimer);
        }
        filterTimer = setTimeout(() => {
            loadShipments();
        }, 250);
    }

    function buildProductsTableHTML(products = []) {
        const totals = {
            q_total: 0,
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
                CHANNELS.forEach((channel) => {
                    totals[channel] += Number(product[channel] || 0);
                });
                return `
                    <tr>
                        <td>${product.producto || ""}</td>
                        <td>${product.marca || ""}</td>
                        <td>${product.sku || ""}</td>
                        <td>${product.q_total || 0}</td>
                        <td>${product.retail || 0}</td>
                        <td>${product.resellers || 0}</td>
                        <td>${product.corporativo || 0}</td>
                        <td>${product.ecommerce || 0}</td>
                        <td>${product.telcom || 0}</td>
                        <td>${product.libre || 0}</td>
                    </tr>
                `;
            })
            .join("");

        return `
            <div class="expand-content">
                <div class="small-text">Productos y distribución por canal</div>
                <div class="table-wrap">
                    <table class="mini-table">
                        <thead>
                            <tr>
                                <th>Producto</th>
                                <th>Marca</th>
                                <th>SKU</th>
                                <th>Q Total</th>
                                <th>Retail</th>
                                <th>Resellers</th>
                                <th>Corporativo</th>
                                <th>Ecommerce</th>
                                <th>Telcom</th>
                                <th>Libre</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${rowsHtml || `<tr><td colspan="10">Sin productos</td></tr>`}
                        </tbody>
                        <tfoot>
                            <tr>
                                <td colspan="3">Totales</td>
                                <td>${totals.q_total}</td>
                                <td>${totals.retail}</td>
                                <td>${totals.resellers}</td>
                                <td>${totals.corporativo}</td>
                                <td>${totals.ecommerce}</td>
                                <td>${totals.telcom}</td>
                                <td>${totals.libre}</td>
                            </tr>
                        </tfoot>
                    </table>
                </div>
            </div>
        `;
    }

    function renderTable(data) {
        shipmentsBody.innerHTML = "";
        if (selectAll) {
            selectAll.checked = false;
        }
        data.forEach((shipment) => {
            const row = document.createElement("tr");
            row.innerHTML = `
                <td><input type="checkbox" class="row-select" data-id="${shipment.id}"></td>
                <td><button class="expand-toggle" data-id="${shipment.id}" title="Ver productos"><i class="fas fa-chevron-down"></i></button></td>
                <td>${shipment.imp || ""}</td>
                <td>${shipment.proveedor || ""}</td>
                <td>${shipment.estado_imp || ""}</td>
                <td>${shipment.tipo_compra || "Sin definir"}</td>
                <td>${shipment.fecha_llegada || ""}</td>
                <td>${shipment.total_qty || 0}</td>
                <td>
                    <button class="ghost-btn small edit-btn" data-id="${shipment.id}"><i class="fas fa-pen"></i></button>
                    <button class="danger-btn small delete-btn" data-id="${shipment.id}"><i class="fas fa-trash"></i></button>
                </td>
            `;
            shipmentsBody.appendChild(row);

            const detailsRow = document.createElement("tr");
            detailsRow.className = "expand-row hidden";
            detailsRow.setAttribute("data-id", shipment.id);
            detailsRow.innerHTML = `
                <td colspan="9">
                    <div class="detail-empty">Abre para cargar productos.</div>
                </td>
            `;
            shipmentsBody.appendChild(detailsRow);
        });

        shipmentsBody.querySelectorAll(".expand-toggle").forEach((btn) => {
            btn.addEventListener("click", async () => {
                const id = btn.getAttribute("data-id");
                const detailRow = shipmentsBody.querySelector(`.expand-row[data-id="${id}"]`);
                if (!detailRow) return;
                detailRow.classList.toggle("hidden");
                if (!detailRow.classList.contains("hidden")) {
                    const cached = shipmentDetails.get(id);
                    if (cached) {
                        detailRow.innerHTML = `<td colspan="9">${buildProductsTableHTML(cached.productos || [])}</td>`;
                    } else {
                        detailRow.innerHTML = `<td colspan="9"><div class="detail-empty">Cargando productos...</div></td>`;
                        const detail = await loadShipmentDetail(id);
                        detailRow.innerHTML = `<td colspan="9">${buildProductsTableHTML(detail?.productos || [])}</td>`;
                    }
                }
                const icon = btn.querySelector("i");
                if (icon) {
                    icon.className = detailRow.classList.contains("hidden")
                        ? "fas fa-chevron-down"
                        : "fas fa-chevron-up";
                }
            });
        });

        shipmentsBody.querySelectorAll(".edit-btn").forEach((btn) => {
            btn.addEventListener("click", async () => {
                const id = btn.getAttribute("data-id");
                let shipment = shipments.find((s) => s.id === id);
                if (!shipment) return;
                const detail = await loadShipmentDetail(id);
                shipment = { ...shipment, ...(detail || {}) };
                if (!shipment) return;
                editingId = id;
                formTitle.textContent = `Editar embarque ${shipment.imp}`;
                shipmentForm.elements.imp.value = shipment.imp || "";
                shipmentForm.elements.proveedor.value = shipment.proveedor || "";
                setStatusSelectValue(shipmentForm.elements.estado_imp, shipment.estado_imp || "");
                shipmentForm.elements.tipo_compra.value = shipment.tipo_compra || "";
                shipmentForm.elements.fecha_llegada.value = shipment.fecha_llegada || "";
                productsBody.innerHTML = "";
                const products = shipment.productos || [];
                products.forEach((product) => {
                    productsBody.appendChild(createProductRow(product));
                });
                if (!products.length) {
                    productsBody.appendChild(createProductRow());
                }
                openModal();
            });
        });

        shipmentsBody.querySelectorAll(".delete-btn").forEach((btn) => {
            btn.addEventListener("click", async () => {
                const id = btn.getAttribute("data-id");
                if (!confirm("Eliminar este embarque?")) return;
                await window.App.authFetch(`/admin/api/shipments/${id}`, { method: "DELETE" });
                shipmentDetails.delete(id);
                await loadShipments();
                resetForm();
            });
        });
    }

    async function loadShipmentDetail(id) {
        if (shipmentDetails.has(id)) {
            return shipmentDetails.get(id);
        }
        const response = await window.App.authFetch(`/admin/api/shipments/${id}`);
        const data = await response.json();
        const detail = data.data || null;
        if (detail) {
            shipmentDetails.set(id, detail);
        }
        return detail;
    }

    async function loadShipments() {
        const params = new URLSearchParams();
        if (filters.imp.value.trim()) params.set("imp", filters.imp.value.trim());
        if (filters.proveedor.value.trim()) params.set("proveedor", filters.proveedor.value.trim());
        if (filters.producto.value.trim()) params.set("producto", filters.producto.value.trim());
        if (filters.marca.value.trim()) params.set("marca", filters.marca.value.trim());
        if (filters.sku.value.trim()) params.set("sku", filters.sku.value.trim());
        if (filters.fecha.value) params.set("fecha", filters.fecha.value);
        if (filters.estado.value.trim()) params.set("estado", filters.estado.value.trim());
        if (filters.tipoCompra.value.trim()) params.set("tipo_compra", filters.tipoCompra.value.trim());
        const query = params.toString();
        const response = await window.App.authFetch(
            `/admin/api/shipments-summary${query ? `?${query}` : ""}`
        );
        const data = await response.json();
        shipments = data.data || [];
        renderTable(shipments);
    }

    addProductRowBtn.addEventListener("click", () => {
        productsBody.appendChild(createProductRow());
    });

    shipmentForm.addEventListener("submit", async (event) => {
        event.preventDefault();
        const payload = {
            imp: shipmentForm.elements.imp.value,
            proveedor: shipmentForm.elements.proveedor.value,
            estado_imp: shipmentForm.elements.estado_imp.value,
            tipo_compra: shipmentForm.elements.tipo_compra.value,
            fecha_llegada: shipmentForm.elements.fecha_llegada.value,
            productos: collectProducts(),
        };

        if (editingId) {
            await window.App.authFetch(`/admin/api/shipments/${editingId}`, {
                method: "PUT",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload),
            });
            shipmentDetails.delete(editingId);
        } else {
            await window.App.authFetch("/admin/api/shipments", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload),
            });
        }
        await loadShipments();
        resetForm();
        closeModal();
    });

    resetFormBtn.addEventListener("click", () => resetForm());

    Object.values(filters).forEach((input) => {
        input.addEventListener("input", applyFilters);
        input.addEventListener("change", applyFilters);
    });

    document.getElementById("clearFilters").addEventListener("click", () => {
        Object.values(filters).forEach((input) => {
            input.value = "";
        });
        loadShipments();
    });

    if (selectAll) {
        selectAll.addEventListener("change", () => {
            document.querySelectorAll(".row-select").forEach((checkbox) => {
                checkbox.checked = selectAll.checked;
            });
        });
    }

    bulkDeleteBtn.addEventListener("click", async () => {
        const ids = Array.from(document.querySelectorAll(".row-select:checked")).map((c) =>
            c.getAttribute("data-id")
        );
        if (!ids.length) return;
        if (!confirm("Eliminar embarques seleccionados?")) return;
        await window.App.authFetch("/admin/api/shipments/bulk-delete", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ ids }),
        });
        ids.forEach((id) => shipmentDetails.delete(id));
        await loadShipments();
        resetForm();
    });

    exportExcelBtn.addEventListener("click", () => {
        window.location.href = "/admin/api/export-excel";
    });

    templateBtn.addEventListener("click", () => {
        window.location.href = "/admin/api/template-excel";
    });

    exportJsonBtn.addEventListener("click", async () => {
        const response = await window.App.authFetch("/admin/api/export-json");
        const data = await response.json();
        const blob = new Blob([JSON.stringify(data.data || [], null, 2)], { type: "application/json" });
        const link = document.createElement("a");
        link.href = URL.createObjectURL(blob);
        link.download = "embarques.json";
        document.body.appendChild(link);
        link.click();
        link.remove();
    });

    importForm.addEventListener("submit", async (event) => {
        event.preventDefault();
        importStatus.textContent = "Procesando...";
        const file = document.getElementById("importFile").files[0];
        if (!file) return;
        const formData = new FormData();
        formData.append("file", file);
        const response = await window.App.authFetch("/admin/api/import-excel", {
            method: "POST",
            body: formData,
        });
        let data = {};
        try {
            data = await response.json();
        } catch (err) {
            data = {};
        }
        if (response.ok) {
            const result = data.data || {};
            importStatus.textContent = `Importado. Filas: ${result.rows || 0}. Nuevos: ${result.created || 0}. Actualizados: ${result.updated || 0}.`;
        } else {
            importStatus.textContent = data.error || "Error importando.";
        }
        await loadShipments();
    });

    if (newShipmentBtn) {
        newShipmentBtn.addEventListener("click", () => {
            resetForm();
            openModal();
        });
    }

    if (closeShipmentModalBtn) {
        closeShipmentModalBtn.addEventListener("click", () => closeModal());
    }

    if (shipmentModal) {
        shipmentModal.addEventListener("click", (event) => {
            if (event.target && event.target.matches("[data-close]")) {
                closeModal();
            }
        });
    }

    document.addEventListener("keydown", (event) => {
        if (event.key === "Escape" && shipmentModal?.classList.contains("open")) {
            closeModal();
        }
    });

    resetForm();
    loadShipments();
});
