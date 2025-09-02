import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re
import io
import os
from datetime import datetime


def extraer_datos_rindegastos(url):
    """
    Extrae datos de RindeGastos manejando correctamente el PDF
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
        }

        print(f"🔍 Accediendo a: {url}")

        # Obtener la página principal
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')

        resultado = {
            'descripcion': "No encontrada",
            'folio_fiscal': "No encontrado",
            'fecha_factura': "No encontrada"
        }

        # MÉTODO 1: Buscar enlace de descarga del PDF
        # Buscar enlaces que contengan "Descargar" o apunten a PDFs
        enlaces_descarga = []

        # Buscar enlaces con texto "Descargar"
        for link in soup.find_all('a'):
            texto_link = link.get_text(strip=True)
            href = link.get('href', '')

            if 'descargar' in texto_link.lower() or 'download' in texto_link.lower():
                if href:
                    enlaces_descarga.append(href)
            elif href and ('.pdf' in href.lower() or 'download' in href.lower()):
                enlaces_descarga.append(href)

        # También intentar construir URLs de descarga comunes
        receipt_match = re.search(r'i=(\d+)', url)
        key_match = re.search(r'key=([^&]+)', url)

        if receipt_match:
            receipt_id = receipt_match.group(1)
            if key_match:
                key = key_match.group(1)

                # URLs de descarga comunes para RindeGastos
                enlaces_descarga.extend([
                    f"https://web.rindegastos.com/document/receipt?i={receipt_id}&key={key}&download=1",
                    f"https://web.rindegastos.com/document/download/{receipt_id}",
                    f"https://web.rindegastos.com/download/{receipt_id}?key={key}",
                    url + "&download=1",
                    url + "&format=pdf",
                ])

        # Intentar descargar el PDF
        pdf_procesado = False

        for enlace in enlaces_descarga:
            if not enlace.startswith('http'):
                enlace = 'https://web.rindegastos.com' + enlace

            try:
                print(f"   📥 Intentando descargar: {enlace}")

                # Descargar con headers específicos para PDFs
                pdf_headers = headers.copy()
                pdf_headers.update({
                    'Accept': 'application/pdf,application/octet-stream,*/*',
                    'Referer': url
                })

                pdf_response = requests.get(enlace, headers=pdf_headers, timeout=30)

                if pdf_response.status_code == 200:
                    content_type = pdf_response.headers.get('content-type', '')
                    content_length = len(pdf_response.content)

                    print(f"   📊 Respuesta: {content_type}, {content_length} bytes")

                    # Verificar si es un PDF válido
                    if content_length > 1000 and (
                            'pdf' in content_type.lower() or
                            pdf_response.content.startswith(b'%PDF')
                    ):
                        print(f"   ✅ PDF válido encontrado!")

                        # Intentar extraer texto del PDF
                        try:
                            # Opción 1: pdfplumber (mejor)
                            try:
                                import pdfplumber

                                with pdfplumber.open(io.BytesIO(pdf_response.content)) as pdf:
                                    texto_completo = ""

                                    for page_num, page in enumerate(pdf.pages):
                                        print(f"   📄 Procesando página {page_num + 1}")

                                        # Extraer texto normal
                                        texto_pagina = page.extract_text()
                                        if texto_pagina:
                                            texto_completo += texto_pagina + "\n"

                                        # Extraer tablas
                                        tablas = page.extract_tables()
                                        for tabla in tablas:
                                            for fila in tabla:
                                                if fila:
                                                    fila_texto = " | ".join(
                                                        [str(celda) if celda else "" for celda in fila])
                                                    texto_completo += fila_texto + "\n"

                                if texto_completo.strip():
                                    resultado = procesar_texto_factura(texto_completo)
                                    pdf_procesado = True
                                    break

                            except ImportError:
                                print(f"   ⚠️ pdfplumber no disponible, intentando PyPDF2...")

                                # Opción 2: PyPDF2 (fallback)
                                try:
                                    import PyPDF2

                                    pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_response.content))
                                    texto_completo = ""

                                    for page_num, page in enumerate(pdf_reader.pages):
                                        print(f"   📄 Procesando página {page_num + 1}")
                                        texto_pagina = page.extract_text()
                                        texto_completo += texto_pagina + "\n"

                                    if texto_completo.strip():
                                        resultado = procesar_texto_factura(texto_completo)
                                        pdf_procesado = True
                                        break

                                except ImportError:
                                    print(f"   ❌ No hay librerías de PDF disponibles")

                        except Exception as pdf_error:
                            print(f"   ❌ Error procesando PDF: {str(pdf_error)}")
                            continue

                    else:
                        print(f"   ⚠️ No parece ser un PDF válido")

                else:
                    print(f"   ❌ Error HTTP: {pdf_response.status_code}")

            except Exception as e:
                print(f"   ❌ Error con enlace {enlace}: {str(e)}")
                continue

        if not pdf_procesado:
            print(f"   ⚠️ No se pudo procesar ningún PDF, intentando extraer de la página HTML...")

            # MÉTODO 2: Intentar extraer datos de la página HTML directamente
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
    Procesa el texto extraído de una factura para encontrar descripción, folio fiscal y fecha
    """
    print(f"   📝 Texto extraído (primeros 300 chars):")
    print(f"   {texto[:300]}...")
    print("   " + "-" * 30)

    resultado = {
        'descripcion': "No encontrada",
        'folio_fiscal': "No encontrado",
        'fecha_factura': "No encontrada"
    }

    # BUSCAR DESCRIPCIÓN
    # Basado en tu imagen, buscar patrones específicos de productos
    patrones_descripcion = [
        # Patrones específicos para tu caso
        r'TERMOPILA[^,\n\r]*(?:MINIVOLTS|HONEYWELL|EN\s*BOLSA)?[^,\n\r]*',
        r'TERMOSTATO[^,\n\r]*(?:RX-\d+|DE\s*\d+.*?FREIDOR)?[^,\n\r]*',

        # Patrones generales
        r'(?:Descripción|Concepto|Producto)[:\s]*([^\n\r]{10,150})',
        r'([A-Z]{4,}[^,\n\r]*(?:HONEYWELL|MINIVOLTS|BOLSA|FREIDOR|TERMOPILA|TERMOSTATO)[^,\n\r]*)',

        # Buscar líneas que parezcan descripciones de productos
        r'^([A-Z][A-Z0-9\s\-\.,/]{15,100}[A-Z0-9])$',
    ]

    for patron in patrones_descripcion:
        matches = re.findall(patron, texto, re.IGNORECASE | re.MULTILINE)
        if matches:
            for match in matches:
                match_limpio = re.sub(r'\s+', ' ', str(match).strip())
                # Verificar que sea una descripción válida
                if (10 <= len(match_limpio) <= 200 and
                        not any(palabra in match_limpio.lower() for palabra in
                                ['folio', 'fiscal', 'certificado', 'serie', 'fecha', 'total', 'subtotal', 'iva'])):
                    resultado['descripcion'] = match_limpio
                    break
            if resultado['descripcion'] != "No encontrada":
                break

    # BUSCAR FOLIO FISCAL
    # Basado en tu imagen, buscar el folio fiscal específico
    patrones_folio = [
        # Formato UUID completo (como se ve en tu imagen)
        r'([A-F0-9]{8}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{12})',

        # Folio fiscal con texto
        r'(?:Folio\s*Fiscal)[:\s]*([A-F0-9-]{20,50})',
        r'(?:UUID)[:\s]*([A-F0-9-]{20,50})',

        # Serie del certificado (alternativo)
        r'(?:Serie\s*del\s*Certificado)[:\s]*([A-Z0-9]{15,})',

        # Número de serie del SAT
        r'(?:No\.\s*de\s*serie)[:\s]*([A-Z0-9]{15,})',
    ]

    for patron in patrones_folio:
        matches = re.findall(patron, texto, re.IGNORECASE)
        if matches:
            for match in matches:
                match_limpio = str(match).strip()
                if len(match_limpio) >= 15:  # Los folios fiscales son largos
                    resultado['folio_fiscal'] = match_limpio
                    break
            if resultado['folio_fiscal'] != "No encontrado":
                break

    # BUSCAR FECHA DE LA FACTURA
    # Múltiples patrones para capturar diferentes formatos de fecha
    patrones_fecha = [
        # Fecha con etiquetas específicas de factura
        r'(?:Fecha\s*de\s*(?:emisión|expedición|factura|comprobante))[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
        r'(?:Fecha\s*y\s*hora\s*de\s*(?:emisión|expedición))[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
        r'(?:Fecha)[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',

        # Formato ISO
        r'(?:Fecha)[:\s]*(\d{4}-\d{2}-\d{2})',

        # Fecha con hora completa (tomar solo la fecha)
        r'(?:Fecha\s*de\s*emisión)[:\s]*(\d{4}-\d{2}-\d{2})T\d{2}:\d{2}:\d{2}',
        r'(?:Fecha)[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s+\d{1,2}:\d{2}',

        # Formatos de fecha con texto en español
        r'(\d{1,2})\s*de\s*(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s*de\s*(\d{2,4})',

        # Formatos generales de fecha (más amplios)
        r'\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\b',
        r'\b(\d{4}[/-]\d{1,2}[/-]\d{1,2})\b',
    ]

    meses_espanol = {
        'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
        'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08',
        'septiembre': '09', 'octubre': '10', 'noviembre': '11', 'diciembre': '12'
    }

    for patron in patrones_fecha:
        matches = re.findall(patron, texto, re.IGNORECASE)
        if matches:
            for match in matches:
                try:
                    if isinstance(match, tuple):  # Para fechas con mes en español
                        dia = match[0].zfill(2)
                        mes = meses_espanol.get(match[1].lower(), match[1])
                        año = match[2]
                        if len(año) == 2:
                            año = '20' + año
                        fecha_str = f"{dia}/{mes}/{año}"
                    else:
                        fecha_str = str(match).strip()

                    # Normalizar el formato de fecha
                    fecha_normalizada = normalizar_fecha(fecha_str)

                    if fecha_normalizada:
                        resultado['fecha_factura'] = fecha_normalizada
                        break

                except Exception as e:
                    print(f"   ⚠️ Error procesando fecha {match}: {str(e)}")
                    continue

            if resultado['fecha_factura'] != "No encontrada":
                break

    return resultado


def normalizar_fecha(fecha_str):
    """
    Normaliza diferentes formatos de fecha a DD/MM/YYYY
    """
    if not fecha_str or fecha_str == "No encontrada":
        return None

    # Limpiar la fecha
    fecha_str = fecha_str.strip()

    # Intentar diferentes formatos
    formatos = [
        '%d/%m/%Y',
        '%d-%m-%Y',
        '%Y-%m-%d',
        '%Y/%m/%d',
        '%d/%m/%y',
        '%d-%m-%y',
        '%m/%d/%Y',
        '%m-%d-%Y',
    ]

    for formato in formatos:
        try:
            fecha_obj = datetime.strptime(fecha_str, formato)
            # Retornar en formato DD/MM/YYYY
            return fecha_obj.strftime('%d/%m/%Y')
        except ValueError:
            continue

    # Si no se pudo parsear con los formatos anteriores, intentar algunos ajustes
    try:
        # Manejar años de 2 dígitos
        if re.match(r'\d{1,2}[/-]\d{1,2}[/-]\d{2}$', fecha_str):
            partes = re.split(r'[/-]', fecha_str)
            if len(partes) == 3:
                partes[2] = '20' + partes[2]
                fecha_str = '/'.join(partes)
                fecha_obj = datetime.strptime(fecha_str, '%d/%m/%Y')
                return fecha_obj.strftime('%d/%m/%Y')
    except:
        pass

    return fecha_str  # Retornar tal cual si no se pudo normalizar


def procesar_facturas_completo(archivo_entrada, archivo_salida):
    """
    Procesa todas las facturas del archivo Excel
    """
    try:
        print("📊 Cargando archivo Excel...")
        df = pd.read_excel(archivo_entrada)

        # Filtrar solo facturas
        facturas = df[df['Tipo de documento'] == 'Factura'].copy()
        print(f"📋 Facturas encontradas: {len(facturas)}")

        if len(facturas) == 0:
            print("❌ No se encontraron facturas")
            return

        # Crear columnas
        if 'Descripción' not in facturas.columns:
            facturas['Descripción'] = ''
        if 'Folio Fiscal Extraído' not in facturas.columns:
            facturas['Folio Fiscal Extraído'] = ''
        if 'Fecha_factura' not in facturas.columns:
            facturas['Fecha_factura'] = ''

        # Procesar cada factura
        exitosas = 0
        errores = 0

        for idx, (_, fila) in enumerate(facturas.iterrows()):
            url = fila['URL']  # Primera columna URL

            if pd.isna(url):
                continue

            print(f"\n{'=' * 60}")
            print(f"📦 Procesando {idx + 1}/{len(facturas)}")
            print(f"🏪 Comercio: {fila['Comercio']}")
            print(f"💰 Total: ${fila['Total']}")

            # Extraer datos
            datos = extraer_datos_rindegastos(url)

            # Guardar resultados
            facturas.at[fila.name, 'Descripción'] = datos['descripcion']
            facturas.at[fila.name, 'Folio Fiscal Extraído'] = datos['folio_fiscal']
            facturas.at[fila.name, 'Fecha_factura'] = datos['fecha_factura']

            if "Error" not in datos['descripcion'] and datos['descripcion'] != "No encontrada":
                exitosas += 1
                print(f"✅ Descripción: {datos['descripcion']}")
                print(f"✅ Folio: {datos['folio_fiscal']}")
                print(f"📅 Fecha: {datos['fecha_factura']}")
            else:
                errores += 1
                print(f"❌ Descripción: {datos['descripcion']}")
                print(f"❌ Folio: {datos['folio_fiscal']}")
                print(f"❌ Fecha: {datos['fecha_factura']}")

            # Pausa
            print("⏳ Esperando 3 segundos...")
            time.sleep(3)

        # Actualizar DataFrame original con los datos extraídos
        df_final = df.copy()

        # Crear columnas si no existen
        if 'Descripción' not in df_final.columns:
            df_final['Descripción'] = ''
        if 'Folio Fiscal Extraído' not in df_final.columns:
            df_final['Folio Fiscal Extraído'] = ''
        if 'Fecha_factura' not in df_final.columns:
            df_final['Fecha_factura'] = ''

        # Actualizar solo las filas de facturas
        for idx, fila in facturas.iterrows():
            df_final.at[idx, 'Descripción'] = fila['Descripción']
            df_final.at[idx, 'Folio Fiscal Extraído'] = fila['Folio Fiscal Extraído']
            df_final.at[idx, 'Fecha_factura'] = fila['Fecha_factura']

        # Guardar archivo
        df_final.to_excel(archivo_salida, index=False)

        print(f"\n{'=' * 60}")
        print(f"🎉 PROCESO COMPLETADO")
        print(f"📁 Archivo guardado: {archivo_salida}")
        print(f"✅ Exitosas: {exitosas}")
        print(f"❌ Errores: {errores}")
        if len(facturas) > 0:
            print(f"📊 Tasa de éxito: {(exitosas / len(facturas) * 100):.1f}%")

        # Mostrar resumen de los datos extraídos
        print(f"\n📋 RESUMEN DE DATOS EXTRAÍDOS:")
        facturas_con_desc = facturas[facturas['Descripción'].str.len() > 10].shape[0]
        facturas_con_folio = facturas[facturas['Folio Fiscal Extraído'].str.len() > 10].shape[0]
        facturas_con_fecha = facturas[(facturas['Fecha_factura'].notna()) &
                                      (facturas['Fecha_factura'] != 'No encontrada')].shape[0]
        print(f"   📝 Facturas con descripción: {facturas_con_desc}")
        print(f"   🔢 Facturas con folio fiscal: {facturas_con_folio}")
        print(f"   📅 Facturas con fecha: {facturas_con_fecha}")

    except Exception as e:
        print(f"❌ Error general: {str(e)}")
        import traceback
        traceback.print_exc()


# PROGRAMA PRINCIPAL
if __name__ == "__main__":
    print("🚀 EXTRACTOR DE FACTURAS RINDEGASTOS")
    print("=" * 50)

    # Solicitar rutas de archivos al usuario
    print("\n📂 CONFIGURACIÓN DE ARCHIVOS")
    print("-" * 50)

    print("\n📥 Por favor, ingresa la ruta completa del archivo Excel de ENTRADA")
    print("   Ejemplo: /Users/gbphy/Downloads/2025_ago_25_Gastos.xlsx")
    print("   (Puedes copiar y pegar la ruta del archivo)")
    archivo_entrada = input("   Ruta del archivo de entrada: ").strip()

    # Verificar si el archivo existe
    while not os.path.exists(archivo_entrada):
        print("   ❌ El archivo no existe. Por favor verifica la ruta.")
        archivo_entrada = input("   Ruta del archivo de entrada: ").strip()

    print("\n📤 Por favor, ingresa la ruta completa para el archivo Excel de SALIDA")
    print("   Ejemplo: /Users/gbphy/Downloads/ERNEST_28.xlsx")
    print("   (Este será el archivo con los datos extraídos)")
    archivo_salida = input("   Ruta del archivo de salida: ").strip()

    # Si no tiene extensión, agregarla
    if not archivo_salida.endswith('.xlsx'):
        archivo_salida += '.xlsx'

    print("\n" + "=" * 50)
    print("📋 RESUMEN DE CONFIGURACIÓN:")
    print(f"📂 Archivo de entrada: {archivo_entrada}")
    print(f"📤 Archivo de salida: {archivo_salida}")
    print("=" * 50)

    # Verificar librerías disponibles
    print("\n📦 VERIFICACIÓN DE LIBRERÍAS:")
    try:
        import pdfplumber

        print("   ✅ pdfplumber disponible")
    except ImportError:
        print("   ⚠️ pdfplumber no disponible")
        try:
            import PyPDF2

            print("   ✅ PyPDF2 disponible como alternativa")
        except ImportError:
            print("   ❌ No hay librerías de PDF disponibles")
            print("   💡 Instala pdfplumber: pip install pdfplumber")

    # Confirmar inicio del proceso
    print("\n" + "=" * 50)
    respuesta = input("¿Procesar todas las facturas? (s/n): ")

    if respuesta.lower() == 's':
        print(f"\n🚀 Iniciando procesamiento...")
        start_time = time.time()

        procesar_facturas_completo(archivo_entrada, archivo_salida)

        end_time = time.time()
        tiempo_total = end_time - start_time
        print(f"\n⏱️ Tiempo total: {tiempo_total / 60:.1f} minutos")

    else:
        print("Proceso cancelado.")