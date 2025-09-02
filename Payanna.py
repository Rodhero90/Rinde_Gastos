import streamlit as st
import pandas as pd
import json
import re
import time
from datetime import datetime, date
import io
import base64
from typing import Dict, List, Optional
import gspread
from google.oauth2.service_account import Credentials
import PyPDF2
import xml.etree.ElementTree as ET
import plotly.express as px
import plotly.graph_objects as go

# Configuración de la página
st.set_page_config(
    page_title="Procesador Payanna",
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personalizado
st.markdown("""
<style>
    .main-header {
        text-align: center;
        padding: 1rem 0;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        color: white;
        border-radius: 10px;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        border-left: 4px solid #667eea;
    }
    .success-box {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        border-radius: 5px;
        padding: 10px;
        margin: 10px 0;
    }
    .warning-box {
        background-color: #fff3cd;
        border: 1px solid #ffeaa7;
        border-radius: 5px;
        padding: 10px;
        margin: 10px 0;
    }
    .error-box {
        background-color: #f8d7da;
        border: 1px solid #f5c6cb;
        border-radius: 5px;
        padding: 10px;
        margin: 10px 0;
    }
</style>
""", unsafe_allow_html=True)


class PayannaProcessorApp:
    def __init__(self):
        self.init_session_state()
        self.load_mock_providers()

    def init_session_state(self):
        """Inicializar variables de sesión"""
        if 'documents' not in st.session_state:
            st.session_state.documents = []
        if 'proveedores_db' not in st.session_state:
            st.session_state.proveedores_db = []
        if 'sheets_connected' not in st.session_state:
            st.session_state.sheets_connected = False
        if 'processing_count' not in st.session_state:
            st.session_state.processing_count = 0

    def load_mock_providers(self):
        """Cargar proveedores de muestra"""
        mock_providers = [
            {
                "codigoUnico": "100BAM", "num": "100", "rfc3": "BAM",
                "rfcProveedor": "BAM911231172", "nombre": "BOLICHES AMF Y COMPAÑIA",
                "razonSocial": "BOLICHES AMF Y COMPAÑIA", "correoProveedor": "fceballos@amf.com",
                "clabe": "0121 8000 1845 4046 84"
            },
            {
                "codigoUnico": "101AAL", "num": "101", "rfc3": "AAL",
                "rfcProveedor": "AALJ0302205H4", "nombre": "JUAN PABLO ALBARRAN LOZA",
                "razonSocial": "JUAN PABLO ALBARRAN LOZA", "correoProveedor": "juanpaloza44@gmail.com",
                "clabe": "0121 8001 5729 5996 56"
            },
            {
                "codigoUnico": "102ACS", "num": "102", "rfc3": "ACS",
                "rfcProveedor": "ACS100122E88", "nombre": "ALIMENTOS CONVENIENTES SAN PATRIC",
                "razonSocial": "ALIMENTOS CONVENIENTES SAN PATRIC, S.A. DE C.V.",
                "correoProveedor": "ngonzales@alimentosconvenientes.com.mx",
                "clabe": "0121 8000 1718 6390 81"
            },
            {
                "codigoUnico": "103AEY", "num": "103", "rfc3": "AEY",
                "rfcProveedor": "AEY1006234R3", "nombre": "APYS ENVASES Y SUMINISTROS SA DE CV",
                "razonSocial": "APYS ENVASES Y SUMINISTROS", "correoProveedor": "cobranza1@envasesysuministros.mx",
                "clabe": "0121 8000 1857 2470 88"
            },
            {
                "codigoUnico": "104AHE", "num": "104", "rfc3": "AHE",
                "rfcProveedor": "AHE181109SJ2", "nombre": "ALL HEROWS",
                "razonSocial": "ALL HEROWS", "correoProveedor": "yolanda@heroguest.com",
                "clabe": "0211 8004 0621 9415 46"
            }
        ]
        st.session_state.proveedores_db = mock_providers

    def setup_google_sheets(self):
        """Configurar Google Sheets (placeholder para configuración real)"""
        with st.expander("🔗 Configuración Google Sheets", expanded=False):
            st.write("**Para conectar con Google Sheets:**")
            st.code("""
# 1. Crear Service Account en Google Cloud Console
# 2. Descargar JSON de credenciales  
# 3. Habilitar Google Sheets API
# 4. Compartir Sheet con email del service account

# En secrets.toml:
[google_sheets_credentials]
type = "service_account"
project_id = "tu-proyecto"
private_key = "-----BEGIN PRIVATE KEY-----..."
client_email = "tu-email@proyecto.iam.gserviceaccount.com"
            """)

            sheet_id = st.text_input("🔑 Google Sheet ID:", placeholder="1BvHNJZBb_dLhvkxQfU5gZM2hW_YrXqPzA8sCdEf")

            if st.button("🔄 Conectar Google Sheets"):
                if sheet_id:
                    with st.spinner("Conectando..."):
                        time.sleep(2)
                        st.success("✅ Conectado exitosamente (simulado)")
                        st.session_state.sheets_connected = True
                else:
                    st.error("Por favor ingresa el Sheet ID")

    def find_proveedor_by_rfc(self, rfc: str) -> Optional[Dict]:
        """Buscar proveedor por RFC"""
        if not rfc:
            return None

        for proveedor in st.session_state.proveedores_db:
            if (proveedor.get("rfcProveedor") == rfc or
                    proveedor.get("rfc3") == rfc[:3] or
                    rfc in proveedor.get("rfcProveedor", "")):
                return proveedor
        return None

    def extract_rfc_from_text(self, text: str) -> Optional[str]:
        """Extraer RFC del texto"""
        rfc_pattern = r'[A-Z&Ñ]{3,4}[0-9]{6}[A-Z0-9]{3}'
        matches = re.findall(rfc_pattern, text.upper())
        return matches[0] if matches else None

    def extract_amount_from_text(self, text: str) -> Optional[str]:
        """Extraer monto del texto"""
        amount_patterns = [
            r'\$[\d,]+\.?\d*',
            r'[\d,]+\.?\d*\s*pesos',
            r'total:?\s*[\d,]+\.?\d*',
        ]

        for pattern in amount_patterns:
            matches = re.findall(pattern, text.lower())
            if matches:
                return matches[0]
        return None

    def process_pdf(self, file) -> Dict:
        """Procesar archivo PDF"""
        try:
            pdf_reader = PyPDF2.PdfReader(file)
            text = ""
            for page in pdf_reader.pages:
                text += page.extract_text()

            rfc = self.extract_rfc_from_text(text)
            monto = self.extract_amount_from_text(text)

            return {
                "rfc": rfc,
                "monto": monto,
                "success": True,
                "text_preview": text[:200] + "..." if len(text) > 200 else text
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def process_xml(self, file) -> Dict:
        """Procesar archivo XML"""
        try:
            xml_content = file.read()
            root = ET.fromstring(xml_content)

            rfc = None
            monto = None

            # Buscar RFC
            for elem in root.iter():
                if 'rfc' in elem.tag.lower() or elem.get('Rfc'):
                    rfc = elem.text or elem.get('Rfc')
                    break

            # Buscar monto
            for elem in root.iter():
                if 'total' in elem.tag.lower() or elem.get('Total'):
                    monto = elem.text or elem.get('Total')
                    if monto:
                        monto = f"${monto}"
                    break

            return {
                "rfc": rfc,
                "monto": monto,
                "success": True,
                "xml_processed": True
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def process_document(self, file) -> Optional[Dict]:
        """Procesar documento completo"""
        # Simular diferentes RFCs para demo
        demo_rfcs = ["BAM911231172", "AALJ0302205H4", "ACS100122E88", "AEY1006234R3", "AHE181109SJ2"]
        simulated_rfc = demo_rfcs[st.session_state.processing_count % len(demo_rfcs)]
        simulated_amount = f"${(st.session_state.processing_count + 1) * 1500 + 500:.2f}"

        # Procesar según tipo de archivo
        if file.type == "application/pdf":
            result = {"rfc": simulated_rfc, "monto": simulated_amount, "success": True}
        elif file.type in ["text/xml", "application/xml"] or file.name.endswith('.xml'):
            result = {"rfc": simulated_rfc, "monto": simulated_amount, "success": True}
        else:
            st.error(f"❌ Tipo de archivo no soportado: {file.type}")
            return None

        if not result.get("success"):
            st.error(f"❌ Error procesando {file.name}: {result.get('error')}")
            return None

        # Buscar proveedor
        rfc = result.get("rfc")
        proveedor = self.find_proveedor_by_rfc(rfc) if rfc else None

        # Crear documento
        document_data = {
            "id": f"{datetime.now().timestamp()}_{file.name}",
            "fileName": file.name,
            "fileType": file.type,
            "fileSize": file.size,
            "timestamp": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            "extractedData": {
                "rfc": rfc or "",
                "rfcProveedor": rfc or "",
                "monto": result.get("monto", ""),
                "codigoUnico": proveedor.get("codigoUnico", "") if proveedor else "",
                "nombreProveedor": proveedor.get("nombre", "") if proveedor else "",
                "razonSocial": proveedor.get("razonSocial", "") if proveedor else "",
                "correoProveedor": proveedor.get("correoProveedor", "") if proveedor else "",
                "clabe": proveedor.get("clabe", "") if proveedor else "",
                "conceptoPago": "",
                "fechaLimite": ""
            },
            "proveedorEncontrado": proveedor is not None
        }

        # Identificar campos faltantes
        required_fields = ["correoProveedor", "clabe", "conceptoPago", "fechaLimite"]
        missing_fields = [field for field in required_fields
                          if not document_data["extractedData"].get(field)]

        document_data["missingFields"] = missing_fields
        document_data["status"] = "complete" if not missing_fields else "needs_input"

        st.session_state.processing_count += 1
        return document_data

    def export_to_payanna(self) -> Optional[str]:
        """Exportar documentos a formato CSV de Payanna"""
        complete_docs = [doc for doc in st.session_state.documents if doc["status"] == "complete"]

        if not complete_docs:
            return None

        data = []
        for doc in complete_docs:
            extracted = doc["extractedData"]
            row = {
                "Timestamp": doc["timestamp"],
                "Nombre solicitante": "Usuario Sistema",
                "Fecha de solicitud": datetime.now().strftime("%d/%m/%Y"),
                "RFC": extracted.get("rfc", ""),
                "Nombre del proveedor": extracted.get("nombreProveedor", ""),
                "Razón Social Proveedor": extracted.get("razonSocial", ""),
                "Correo Proveedor": extracted.get("correoProveedor", ""),
                "Clabe": extracted.get("clabe", ""),
                "Monto": extracted.get("monto", ""),
                "PDF": doc["fileName"] if doc["fileName"].endswith('.pdf') else "",
                "XML": doc["fileName"] if doc["fileName"].endswith('.xml') else "",
                "Concepto de Pago": extracted.get("conceptoPago", ""),
                "Comentario": "",
                "Fecha limite de pago": extracted.get("fechaLimite", "")
            }
            data.append(row)

        df = pd.DataFrame(data)
        return df.to_csv(index=False)

    def render_sidebar(self):
        """Renderizar barra lateral"""
        with st.sidebar:
            st.title("📊 Dashboard")

            # Métricas
            total_docs = len(st.session_state.documents)
            complete_docs = len([d for d in st.session_state.documents if d["status"] == "complete"])
            pending_docs = total_docs - complete_docs
            providers_count = len(st.session_state.proveedores_db)

            col1, col2 = st.columns(2)
            with col1:
                st.metric("📄 Total", total_docs)
                st.metric("✅ Completos", complete_docs)
            with col2:
                st.metric("⚠️ Pendientes", pending_docs)
                st.metric("👥 Proveedores", providers_count)

            # Gráfico de estado
            if total_docs > 0:
                st.subheader("📈 Estado de Documentos")
                fig = px.pie(
                    values=[complete_docs, pending_docs],
                    names=["Completos", "Pendientes"],
                    color_discrete_map={
                        "Completos": "#28a745",
                        "Pendientes": "#ffc107"
                    }
                )
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)

            # Configuración Google Sheets
            st.markdown("---")
            self.setup_google_sheets()

            # Acciones rápidas
            st.markdown("---")
            st.subheader("🚀 Acciones Rápidas")

            if st.button("🗑️ Limpiar Todo", type="secondary"):
                st.session_state.documents = []
                st.experimental_rerun()

            if st.button("📤 Ver Proveedores", type="secondary"):
                st.session_state.show_providers = True

    def render_main_content(self):
        """Renderizar contenido principal"""
        # Header
        st.markdown("""
        <div class="main-header">
            <h1>📄 Procesador Payanna</h1>
            <p>Automatiza el procesamiento de facturas para Payanna</p>
        </div>
        """, unsafe_allow_html=True)

        # Estado de conexión
        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            if st.session_state.sheets_connected:
                st.success("✅ Google Sheets conectado")
            else:
                st.info("ℹ️ Usando datos de muestra")

        with col2:
            st.metric("🏢 Proveedores DB", len(st.session_state.proveedores_db))

        with col3:
            if st.session_state.documents:
                completion_rate = len([d for d in st.session_state.documents if d["status"] == "complete"]) / len(
                    st.session_state.documents) * 100
                st.metric("📊 Completitud", f"{completion_rate:.0f}%")

        st.markdown("---")

        # Zona de carga de archivos
        st.subheader("📁 Subir Documentos")

        uploaded_files = st.file_uploader(
            "Arrastra tus archivos PDF o XML aquí:",
            type=['pdf', 'xml'],
            accept_multiple_files=True,
            help="📝 Soporta archivos PDF y XML. Puedes subir múltiples archivos a la vez."
        )

        # Procesar archivos
        if uploaded_files:
            progress_bar = st.progress(0)
            status_text = st.empty()

            for i, file in enumerate(uploaded_files):
                # Verificar si ya existe
                existing_files = [doc["fileName"] for doc in st.session_state.documents]
                if file.name in existing_files:
                    st.warning(f"⚠️ {file.name} ya fue procesado")
                    continue

                # Procesar archivo
                status_text.text(f"🔄 Procesando {file.name}...")
                progress_bar.progress((i + 1) / len(uploaded_files))

                time.sleep(0.5)  # Simular procesamiento

                document_data = self.process_document(file)
                if document_data:
                    st.session_state.documents.append(document_data)

                    # Mostrar resultado inmediato
                    if document_data["proveedorEncontrado"]:
                        st.success(f"✅ {file.name} - Proveedor encontrado automáticamente")
                    else:
                        st.warning(f"⚠️ {file.name} - Requiere datos adicionales")

            progress_bar.progress(1.0)
            status_text.text("✅ ¡Procesamiento completado!")
            time.sleep(1)
            status_text.empty()
            progress_bar.empty()

        # Mostrar documentos procesados
        if st.session_state.documents:
            st.markdown("---")

            # Header de documentos
            col1, col2, col3 = st.columns([2, 1, 1])
            with col1:
                st.subheader(f"📋 Documentos Procesados ({len(st.session_state.documents)})")

            with col2:
                # Filtro de estado
                filter_status = st.selectbox(
                    "🔍 Filtrar por:",
                    ["Todos", "Completos", "Pendientes"],
                    key="status_filter"
                )

            with col3:
                # Botón de exportación
                complete_count = len([d for d in st.session_state.documents if d["status"] == "complete"])
                if complete_count > 0:
                    csv_data = self.export_to_payanna()
                    if csv_data:
                        st.download_button(
                            label=f"📤 Exportar ({complete_count})",
                            data=csv_data,
                            file_name=f"payanna_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv",
                            type="primary"
                        )
                else:
                    st.button("📤 Exportar (0)", disabled=True)

            # Filtrar documentos
            filtered_docs = st.session_state.documents
            if filter_status == "Completos":
                filtered_docs = [d for d in st.session_state.documents if d["status"] == "complete"]
            elif filter_status == "Pendientes":
                filtered_docs = [d for d in st.session_state.documents if d["status"] == "needs_input"]

            # Mostrar documentos
            for i, doc in enumerate(filtered_docs):
                self.render_document_card(doc, i)

        else:
            # Estado vacío
            st.markdown("""
            <div style="text-align: center; padding: 3rem; background-color: #f8f9fa; border-radius: 10px; margin: 2rem 0;">
                <h3>📂 No hay documentos procesados</h3>
                <p>Sube archivos PDF o XML para comenzar el procesamiento automático</p>
            </div>
            """, unsafe_allow_html=True)

    def render_document_card(self, doc: Dict, index: int):
        """Renderizar tarjeta de documento"""
        with st.container():
            # Header de la tarjeta
            col1, col2, col3 = st.columns([3, 1, 1])

            with col1:
                status_icon = "✅" if doc["status"] == "complete" else "⚠️"
                status_text = "Completo" if doc["status"] == "complete" else "Requiere datos"
                st.write(f"**{status_icon} {doc['fileName']}** - {status_text}")
                st.caption(f"📅 {doc['timestamp']} • 📊 {doc['fileType']} • 💾 {doc['fileSize']} bytes")

            with col2:
                if doc["proveedorEncontrado"]:
                    st.success("🏢 Proveedor encontrado")
                else:
                    st.warning("🔍 Proveedor no encontrado")

            with col3:
                if st.button("🗑️", key=f"delete_{doc['id']}", help="Eliminar documento"):
                    st.session_state.documents = [d for d in st.session_state.documents if d["id"] != doc["id"]]
                    st.experimental_rerun()

            # Expandir detalles
            with st.expander("👁️ Ver detalles", expanded=(doc["status"] == "needs_input")):
                extracted = doc["extractedData"]

                # Datos extraídos
                col1, col2 = st.columns(2)
                with col1:
                    st.write("**📊 Datos Extraídos:**")
                    if extracted.get("rfc"):
                        st.info(f"🆔 RFC: {extracted['rfc']}")
                    if extracted.get("monto"):
                        st.info(f"💰 Monto: {extracted['monto']}")
                    if extracted.get("codigoUnico"):
                        st.info(f"🔢 Código: {extracted['codigoUnico']}")

                with col2:
                    if doc["proveedorEncontrado"]:
                        st.write("**🏢 Datos del Proveedor:**")
                        st.write(f"• **Nombre:** {extracted.get('nombreProveedor', 'N/A')}")
                        st.write(f"• **Email:** {extracted.get('correoProveedor', 'N/A')}")
                        st.write(f"• **CLABE:** {extracted.get('clabe', 'N/A')}")

                # Formulario para campos faltantes
                if doc["missingFields"]:
                    st.write("**✏️ Complete los datos faltantes:**")

                    with st.form(f"form_{doc['id']}"):
                        updated_data = {}

                        col1, col2 = st.columns(2)

                        for field in doc["missingFields"]:
                            target_col = col1 if len(updated_data) % 2 == 0 else col2

                            with target_col:
                                if field == "correoProveedor":
                                    updated_data[field] = st.text_input(
                                        "📧 Correo del Proveedor",
                                        value=extracted.get(field, ""),
                                        key=f"{field}_{doc['id']}"
                                    )
                                elif field == "clabe":
                                    updated_data[field] = st.text_input(
                                        "🏦 CLABE Bancaria",
                                        value=extracted.get(field, ""),
                                        key=f"{field}_{doc['id']}"
                                    )
                                elif field == "fechaLimite":
                                    fecha_value = None
                                    if extracted.get(field):
                                        try:
                                            fecha_value = datetime.strptime(extracted[field], "%Y-%m-%d").date()
                                        except:
                                            pass

                                    updated_data[field] = st.date_input(
                                        "📅 Fecha Límite de Pago",
                                        value=fecha_value,
                                        key=f"{field}_{doc['id']}"
                                    )

                        # Concepto de pago (ancho completo)
                        if "conceptoPago" in doc["missingFields"]:
                            updated_data["conceptoPago"] = st.text_area(
                                "📝 Concepto de Pago",
                                value=extracted.get("conceptoPago", ""),
                                key=f"conceptoPago_{doc['id']}",
                                height=100,
                                placeholder="Ejemplo: Compra de envases para productos..."
                            )

                        # Botones
                        col1, col2 = st.columns(2)
                        with col1:
                            submit_button = st.form_submit_button("💾 Guardar Datos", type="primary")
                        with col2:
                            auto_fill = st.form_submit_button("🤖 Auto-completar", type="secondary")

                        if submit_button:
                            # Actualizar documento
                            for field, value in updated_data.items():
                                if value:
                                    if isinstance(value, date):
                                        doc["extractedData"][field] = value.strftime("%Y-%m-%d")
                                    else:
                                        doc["extractedData"][field] = str(value)

                                    if field in doc["missingFields"]:
                                        doc["missingFields"].remove(field)

                            # Actualizar estado
                            if not doc["missingFields"]:
                                doc["status"] = "complete"

                            st.success("✅ Datos guardados correctamente")
                            st.experimental_rerun()

                        if auto_fill and not doc["proveedorEncontrado"]:
                            # Auto-completar con datos por defecto
                            default_data = {
                                "correoProveedor": "contacto@empresa.com",
                                "clabe": "0000 0000 0000 0000 00",
                                "conceptoPago": "Servicios profesionales",
                                "fechaLimite": (datetime.now().date()).strftime("%Y-%m-%d")
                            }

                            for field in doc["missingFields"]:
                                if field in default_data:
                                    doc["extractedData"][field] = default_data[field]

                            doc["missingFields"] = []
                            doc["status"] = "complete"

                            st.success("🤖 Datos completados automáticamente")
                            st.experimental_rerun()

            st.markdown("---")

    def run(self):
        """Ejecutar la aplicación"""
        self.render_sidebar()
        self.render_main_content()


# Ejecutar aplicación
if __name__ == "__main__":
    app = PayannaProcessorApp()
    app.run()