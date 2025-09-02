import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re
import io
from datetime import datetime
import base64

# Configuración de la página
st.set_page_config(
    page_title="Extractor de Facturas RindeGastos",
    page_icon="📊",
    layout="wide"
)

# CSS personalizado
st.markdown("""
    <style>
    .main {
        padding-top: 2rem;
    }
    .stButton>button {
        width: 100%;
        background-color: #4CAF50;
        color: white;
    }
    .success-box {
        padding: 1rem;
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        border-radius: 5px;
        margin: 1rem 0;
    }
    .error-box {
        padding: 1rem;
        background-color: #f8d7da;
        border: 1px solid #f5c6cb;
        border-radius: 5px;
        margin: 1rem 0;
    }
    </style>
    """, unsafe_allow_html=True)

# Título principal
st.title("🚀 Extractor de Facturas RindeGastos")
st.markdown("---")


# Funciones de procesamiento
@st.cache_data
def extraer_datos_rindegastos(url, progress_callback=None):
    """
    Extrae datos de RindeGastos manejando correctamente el PDF
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
        }

        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        resultado = {
            'descripcion': "No encontrada",
            'folio_fiscal': "No encontrado",
            'fecha_factura': "No encontrada"
        }

        # Buscar enlaces de descarga
        enlaces_descarga = []

        for link in soup.find_all('a'):
            texto_link = link.get_text(strip=True)
            href = link.get('href', '')

            if 'descargar' in texto_link.lower() or 'download' in texto_link.lower():
                if href:
                    enlaces_descarga.append(href)
            elif href and ('.pdf' in href.lower() or 'download' in href.lower()):
                enlaces_descarga.append(href)

        # Construir URLs de descarga comunes
        receipt_match = re.search(r'i=(\d+)', url)
        key_match = re.search(r'key=([^&]+)', url)

        if receipt_match:
            receipt_id = receipt_match.group(1)
            if key_match:
                key = key_match.group(1)
                enlaces_descarga.extend([
                    f"https://web.rindegastos.com/document/receipt?i={receipt_id}&key={key}&download=1",
                    f"https://web.rindegastos.com/document/download/{receipt_id}",
                    url + "&download=1",
                ])

        # Intentar descargar el PDF
        pdf_procesado = False

        for enlace in enlaces_descarga:
            if not enlace.startswith('http'):
                enlace = 'https://web.rindegastos.com' + enlace

            try:
                pdf_headers = headers.copy()
                pdf_headers.update({
                    'Accept': 'application/pdf,application/octet-stream,*/*',
                    'Referer': url
                })

                pdf_response = requests.get(enlace, headers=pdf_headers, timeout=30)

                if pdf_response.status_code == 200:
                    content_length = len(pdf_response.content)

                    if content_length > 1000 and pdf_response.content.startswith(b'%PDF'):
                        try:
                            import pdfplumber
                            with pdfplumber.open(io.BytesIO(pdf_response.content)) as pdf:
                                texto_completo = ""
                                for page in pdf.pages:
                                    texto_pagina = page.extract_text()
                                    if texto_pagina:
                                        texto_completo += texto_pagina + "\n"

                                    tablas = page.extract_tables()
                                    for tabla in tablas:
                                        for fila in tabla:
                                            if fila:
                                                fila_texto = " | ".join([str(celda) if celda else "" for celda in fila])
                                                texto_completo += fila_texto + "\n"

                                if texto_completo.strip():
                                    resultado = procesar_texto_factura(texto_completo)
                                    pdf_procesado = True
                                    break
                        except ImportError:
                            try:
                                import PyPDF2
                                pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_response.content))
                                texto_completo = ""
                                for page in pdf_reader.pages:
                                    texto_pagina = page.extract_text()
                                    texto_completo += texto_pagina + "\n"

                                if texto_completo.strip():
                                    resultado = procesar_texto_factura(texto_completo)
                                    pdf_procesado = True
                                    break
                            except:
                                pass
                        except Exception:
                            continue
            except Exception:
                continue

        if not pdf_procesado:
            texto_html = soup.get_text()
            resultado = procesar_texto_factura(texto_html)

        return resultado

    except Exception as e:
        return {
            'descripcion': f"Error: {str(e)}",
            'folio_fiscal': f"Error: {str(e)}",
            'fecha_factura': f"Error: {str(e)}"
        }


def procesar_texto_factura(texto):
    """
    Procesa el texto extraído de una factura
    """
    resultado = {
        'descripcion': "No encontrada",
        'folio_fiscal': "No encontrado",
        'fecha_factura': "No encontrada"
    }

    # BUSCAR DESCRIPCIÓN
    patrones_descripcion = [
        r'TERMOPILA[^,\n\r]*(?:MINIVOLTS|HONEYWELL|EN\s*BOLSA)?[^,\n\r]*',
        r'TERMOSTATO[^,\n\r]*(?:RX-\d+|DE\s*\d+.*?FREIDOR)?[^,\n\r]*',
        r'(?:Descripción|Concepto|Producto)[:\s]*([^\n\r]{10,150})',
        r'([A-Z]{4,}[^,\n\r]*(?:HONEYWELL|MINIVOLTS|BOLSA|FREIDOR|TERMOPILA|TERMOSTATO)[^,\n\r]*)',
    ]

    for patron in patrones_descripcion:
        matches = re.findall(patron, texto, re.IGNORECASE | re.MULTILINE)
        if matches:
            for match in matches:
                match_limpio = re.sub(r'\s+', ' ', str(match).strip())
                if (10 <= len(match_limpio) <= 200 and
                        not any(palabra in match_limpio.lower() for palabra in
                                ['folio', 'fiscal', 'certificado', 'serie', 'fecha', 'total', 'subtotal', 'iva'])):
                    resultado['descripcion'] = match_limpio
                    break
            if resultado['descripcion'] != "No encontrada":
                break

    # BUSCAR FOLIO FISCAL
    patrones_folio = [
        r'([A-F0-9]{8}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{12})',
        r'(?:Folio\s*Fiscal)[:\s]*([A-F0-9-]{20,50})',
        r'(?:UUID)[:\s]*([A-F0-9-]{20,50})',
    ]

    for patron in patrones_folio:
        matches = re.findall(patron, texto, re.IGNORECASE)
        if matches:
            for match in matches:
                match_limpio = str(match).strip()
                if len(match_limpio) >= 15:
                    resultado['folio_fiscal'] = match_limpio
                    break
            if resultado['folio_fiscal'] != "No encontrado":
                break

    # BUSCAR FECHA DE LA FACTURA
    patrones_fecha = [
        r'(?:Fecha\s*y\s*hora\s*de\s*(?:emisión|expedición|certificación))[:\s]*(\d{4}-\d{2}-\d{2})',
        r'(?:Fecha\s*de\s*(?:emisión|expedición|factura|comprobante|certificación))[:\s]*(\d{4}-\d{2}-\d{2})',
        r'(?:Fecha)[:\s]*(\d{4}-\d{2}-\d{2})',
        r'(?:Fecha)[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
        r'(\d{4}-\d{2}-\d{2})T\d{2}:\d{2}:\d{2}',
        r'\b(\d{1,2}/\d{1,2}/\d{4})\b',
        r'\b(\d{1,2}-\d{1,2}-\d{4})\b',
    ]

    for patron in patrones_fecha:
        matches = re.findall(patron, texto, re.IGNORECASE | re.MULTILINE)
        if matches:
            fecha_str = str(matches[0]).strip()
            fecha_normalizada = normalizar_fecha(fecha_str)
            if fecha_normalizada:
                resultado['fecha_factura'] = fecha_normalizada
                break

    return resultado


def normalizar_fecha(fecha_str):
    """Normaliza diferentes formatos de fecha a DD/MM/YYYY"""
    if not fecha_str:
        return None

    fecha_str = fecha_str.strip().replace('T', ' ').split(' ')[0]

    formatos = [
        '%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y',
        '%Y/%m/%d', '%d/%m/%y', '%d-%m-%y',
        '%m/%d/%Y', '%m-%d-%Y'
    ]

    for formato in formatos:
        try:
            fecha_obj = datetime.strptime(fecha_str, formato)
            if 1900 <= fecha_obj.year <= 2100:
                return fecha_obj.strftime('%d/%m/%Y')
        except ValueError:
            continue

    return None


def get_table_download_link(df):
    """Genera un link de descarga para el DataFrame"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Facturas')
    excel_data = output.getvalue()
    b64 = base64.b64encode(excel_data).decode()
    href = f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="facturas_procesadas.xlsx">📥 Descargar Excel</a>'
    return href


# ==================== INTERFAZ PRINCIPAL DE STREAMLIT ====================

# Sidebar con información
with st.sidebar:
    st.header("📋 Instrucciones")
    st.markdown("""
    1. **Carga tu archivo Excel** con las URLs de RindeGastos
    2. **Revisa la vista previa** de los datos
    3. **Haz clic en "Procesar Facturas"**
    4. **Descarga el resultado** con los datos extraídos

    ---

    ### 📊 Columnas requeridas:
    - `URL`: Enlaces de RindeGastos
    - `Tipo de documento`: Para filtrar facturas
    - `Comercio`: Nombre del comercio
    - `Total`: Monto total

    ---

    ### 🎯 Datos extraídos:
    - Descripción del producto/servicio
    - Folio fiscal (UUID)
    - Fecha de la factura
    """)

    st.info("💡 El proceso toma ~3 segundos por factura")

# Área principal
col1, col2 = st.columns([2, 1])

with col1:
    st.header("📁 Cargar archivo Excel")
    uploaded_file = st.file_uploader(
        "Selecciona tu archivo Excel con las URLs de RindeGastos",
        type=['xlsx', 'xls'],
        help="El archivo debe contener una columna 'URL' con los enlaces de RindeGastos"
    )

with col2:
    st.header("⚙️ Configuración")
    delay_time = st.slider(
        "Tiempo de espera entre facturas (segundos)",
        min_value=1,
        max_value=10,
        value=3,
        help="Ajusta el tiempo de espera para evitar bloqueos"
    )

if uploaded_file is not None:
    # Leer el archivo
    try:
        df = pd.read_excel(uploaded_file)

        # Verificar columnas requeridas
        columnas_requeridas = ['URL', 'Tipo de documento']
        columnas_faltantes = [col for col in columnas_requeridas if col not in df.columns]

        if columnas_faltantes:
            st.error(f"❌ Columnas faltantes: {', '.join(columnas_faltantes)}")
        else:
            # Filtrar solo facturas
            facturas = df[df['Tipo de documento'] == 'Factura'].copy()
            total_facturas = len(facturas)

            # Mostrar estadísticas
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total de registros", len(df))
            with col2:
                st.metric("Facturas encontradas", total_facturas)
            with col3:
                tiempo_estimado = total_facturas * delay_time
                st.metric("Tiempo estimado", f"{tiempo_estimado // 60}:{tiempo_estimado % 60:02d} min")

            # Vista previa de datos
            with st.expander("👁️ Vista previa de las facturas", expanded=True):
                columnas_mostrar = ['URL', 'Comercio', 'Total'] if 'Comercio' in facturas.columns else ['URL']
                st.dataframe(
                    facturas[columnas_mostrar].head(10),
                    use_container_width=True,
                    hide_index=True
                )

            # Botón de procesamiento
            if st.button("🚀 Procesar Facturas", type="primary", use_container_width=True):
                if total_facturas == 0:
                    st.warning("⚠️ No hay facturas para procesar")
                else:
                    # Crear columnas para resultados
                    facturas['Descripción'] = ''
                    facturas['Folio Fiscal Extraído'] = ''
                    facturas['Fecha_factura'] = ''

                    # Barra de progreso
                    progress_bar = st.progress(0)
                    status_text = st.empty()

                    # Contenedor para logs
                    log_container = st.container()

                    # Procesar cada factura
                    exitosas = 0
                    errores = 0

                    for idx, (index, fila) in enumerate(facturas.iterrows()):
                        url = fila['URL']

                        if pd.isna(url):
                            continue

                        # Actualizar progreso
                        progreso = (idx + 1) / total_facturas
                        progress_bar.progress(progreso)
                        status_text.text(
                            f"Procesando {idx + 1}/{total_facturas} - {fila.get('Comercio', 'Sin nombre')}")

                        # Extraer datos
                        with st.spinner(f"Extrayendo datos de factura {idx + 1}..."):
                            datos = extraer_datos_rindegastos(url)

                        # Guardar resultados
                        facturas.at[index, 'Descripción'] = datos['descripcion']
                        facturas.at[index, 'Folio Fiscal Extraído'] = datos['folio_fiscal']
                        facturas.at[index, 'Fecha_factura'] = datos['fecha_factura']

                        # Actualizar contadores y mostrar resultado
                        if "Error" not in datos['descripcion'] and datos['descripcion'] != "No encontrada":
                            exitosas += 1
                            with log_container:
                                st.success(f"✅ Factura {idx + 1}: {datos['descripcion'][:50]}...")
                        else:
                            errores += 1
                            with log_container:
                                st.error(f"❌ Factura {idx + 1}: No se pudo extraer información")

                        # Esperar antes de la siguiente
                        if idx < total_facturas - 1:
                            time.sleep(delay_time)

                    # Actualizar DataFrame original
                    df_final = df.copy()
                    for col in ['Descripción', 'Folio Fiscal Extraído', 'Fecha_factura']:
                        if col not in df_final.columns:
                            df_final[col] = ''

                    for index, fila in facturas.iterrows():
                        df_final.at[index, 'Descripción'] = fila['Descripción']
                        df_final.at[index, 'Folio Fiscal Extraído'] = fila['Folio Fiscal Extraído']
                        df_final.at[index, 'Fecha_factura'] = fila['Fecha_factura']

                    # Mostrar resultados finales
                    st.markdown("---")
                    st.header("📊 Resultados del Procesamiento")

                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("✅ Exitosas", exitosas, delta=f"{exitosas / total_facturas * 100:.1f}%")
                    with col2:
                        st.metric("❌ Errores", errores)
                    with col3:
                        facturas_con_desc = len(facturas[facturas['Descripción'].str.len() > 10])
                        st.metric("📝 Con descripción", facturas_con_desc)
                    with col4:
                        facturas_con_folio = len(facturas[facturas['Folio Fiscal Extraído'].str.len() > 10])
                        st.metric("🔢 Con folio", facturas_con_folio)

                    # Vista previa de resultados
                    with st.expander("📋 Vista previa de resultados", expanded=True):
                        columnas_resultado = ['Comercio', 'Total', 'Descripción', 'Folio Fiscal Extraído',
                                              'Fecha_factura']
                        columnas_disponibles = [col for col in columnas_resultado if col in df_final.columns]
                        st.dataframe(
                            df_final[df_final['Tipo de documento'] == 'Factura'][columnas_disponibles].head(20),
                            use_container_width=True,
                            hide_index=True
                        )

                    # Guardar en session state para descarga
                    st.session_state['df_procesado'] = df_final

                    # Botón de descarga
                    st.markdown("### 📥 Descargar Resultados")
                    st.markdown(get_table_download_link(df_final), unsafe_allow_html=True)

                    # Mensaje de éxito
                    st.balloons()
                    st.success(f"🎉 Proceso completado exitosamente! Se procesaron {total_facturas} facturas.")

    except Exception as e:
        st.error(f"❌ Error al leer el archivo: {str(e)}")
        st.info("Por favor, verifica que el archivo sea un Excel válido con las columnas requeridas.")

else:
    # Mensaje de bienvenida cuando no hay archivo
    st.info("👆 Por favor, carga un archivo Excel para comenzar")

    # Ejemplo de formato esperado
    with st.expander("📄 Ver formato de archivo esperado"):
        ejemplo_df = pd.DataFrame({
            'URL': [
                'https://web.rindegastos.com/document/receipt?i=12345&key=abc123',
                'https://web.rindegastos.com/document/receipt?i=67890&key=def456'
            ],
            'Tipo de documento': ['Factura', 'Factura'],
            'Comercio': ['Empresa ABC', 'Empresa XYZ'],
            'Total': [1500.00, 2300.50]
        })
        st.dataframe(ejemplo_df, use_container_width=True, hide_index=True)

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: gray;'>
        <small>
            Extractor de Facturas RindeGastos v1.0 | 
            Desarrollado para automatizar la extracción de datos fiscales
        </small>
    </div>
    """,
    unsafe_allow_html=True
)