import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re
import io
import pdfplumber
import PyPDF2
from datetime import datetime
import urllib.parse
import xml.etree.ElementTree as ET
import os
from pathlib import Path


class ExtractorFacturasRindeGastosV7:
    """
    Versión 7 con búsqueda adicional en carpeta local de XMLs
    """

    def __init__(self, carpeta_cfdi=None):
        self.session = requests.Session()
        self.carpeta_cfdi = carpeta_cfdi

        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }

        self.session.headers.update(self.headers)

        # Palabras que NO son productos (filtros mejorados)
        self.palabras_excluir = [
            'NÚMERODEPEDIMENTO', 'NUMERODEPEDIMENTO', 'P. UNITARIO', 'UNITARIO',
            'CANTIDAD', 'UNIDAD', 'PRECIO', 'IMPORTE', 'DESCUENTO', 'SUBTOTAL',
            'TOTAL', 'IVA', 'FOLIO', 'FECHA', 'RFC', 'SERIE', 'CERTIFICADO',
            'FACTURA', 'CLIENTE', 'PROVEEDOR', 'EMISOR', 'RECEPTOR',
            'DATOS GENERALES', 'INFORMACIÓN', 'CFDI', 'SAT', 'TIMBRE',
            'CADENA ORIGINAL', 'SELLO DIGITAL', 'TIPO DE COMPROBANTE',
            'NO ENCONTRADA', 'MÉTODO DE PAGO', 'FORMA DE PAGO', 'USO CFDI'
        ]

    def buscar_xml_local(self, comercio, fecha, total):
        """
        Busca el XML correspondiente en la carpeta local
        """
        if not self.carpeta_cfdi or not os.path.exists(self.carpeta_cfdi):
            return None

        print(f"   🔍 Buscando XML local para: {comercio}, Fecha: {fecha}, Total: ${total:.2f}")

        # Convertir fecha a objeto datetime para comparaciones
        try:
            if isinstance(fecha, str):
                fecha_factura = datetime.strptime(fecha, "%Y-%m-%d")
            else:
                fecha_factura = fecha
        except:
            fecha_factura = None

        archivos_xml = []

        # Buscar todos los XMLs en la carpeta
        for archivo in Path(self.carpeta_cfdi).glob("*.xml"):
            try:
                tree = ET.parse(archivo)
                root = tree.getroot()

                # Namespaces comunes en CFDI
                ns = {
                    'cfdi': 'http://www.sat.gob.mx/cfd/4',
                    'cfdi3': 'http://www.sat.gob.mx/cfd/3',
                    'tfd': 'http://www.sat.gob.mx/TimbreFiscalDigital'
                }

                # Intentar con CFDI 4.0
                comprobante = root.find('.', ns)
                if comprobante is None:
                    # Intentar con CFDI 3.3
                    ns['cfdi'] = ns['cfdi3']
                    comprobante = root

                # Extraer datos del XML
                total_xml = float(comprobante.get('Total', '0'))
                fecha_xml = comprobante.get('Fecha', '')

                # Nombre del emisor
                emisor = root.find('.//cfdi:Emisor', ns)
                if emisor is not None:
                    nombre_emisor = emisor.get('Nombre', '')
                else:
                    nombre_emisor = ''

                # Comparar por total (con tolerancia de centavos)
                if abs(total_xml - total) < 0.10:
                    archivos_xml.append({
                        'archivo': archivo,
                        'total': total_xml,
                        'fecha': fecha_xml,
                        'emisor': nombre_emisor,
                        'coincidencia_total': True
                    })
                    print(f"      ✅ Coincidencia por total encontrada: {archivo.name}")

                # Si no hay coincidencia por total, buscar por fecha y nombre similar
                elif fecha_factura and fecha_xml:
                    try:
                        fecha_xml_dt = datetime.strptime(fecha_xml[:10], "%Y-%m-%d")
                        # Si la fecha coincide y el nombre es similar
                        if fecha_xml_dt.date() == fecha_factura.date():
                            if comercio.upper() in nombre_emisor.upper() or nombre_emisor.upper() in comercio.upper():
                                archivos_xml.append({
                                    'archivo': archivo,
                                    'total': total_xml,
                                    'fecha': fecha_xml,
                                    'emisor': nombre_emisor,
                                    'coincidencia_total': False
                                })
                                print(f"      ⚠️ Posible coincidencia por fecha/nombre: {archivo.name}")
                    except:
                        pass

            except Exception as e:
                print(f"      ❌ Error leyendo {archivo.name}: {str(e)[:50]}")
                continue

        # Si encontramos archivos, retornar el mejor match
        if archivos_xml:
            # Priorizar coincidencias por total
            archivos_xml.sort(key=lambda x: (x['coincidencia_total'], -x['total']), reverse=True)
            return archivos_xml[0]['archivo']

        return None

    def procesar_xml_cfdi(self, archivo_xml):
        """
        Procesa un archivo XML de CFDI para extraer descripción y folio fiscal
        """
        try:
            tree = ET.parse(archivo_xml)
            root = tree.getroot()

            # Namespaces
            ns = {
                'cfdi': 'http://www.sat.gob.mx/cfd/4',
                'cfdi3': 'http://www.sat.gob.mx/cfd/3',
                'tfd': 'http://www.sat.gob.mx/TimbreFiscalDigital'
            }

            # Determinar versión
            version = root.get('Version', '4.0')
            if version.startswith('3'):
                ns['cfdi'] = ns['cfdi3']

            resultado = {
                'descripcion': "No encontrada",
                'folio_fiscal': "No encontrado"
            }

            # Buscar UUID/Folio Fiscal
            timbre = root.find('.//tfd:TimbreFiscalDigital', ns)
            if timbre is not None:
                resultado['folio_fiscal'] = timbre.get('UUID', 'No encontrado')

            # Buscar conceptos/productos
            conceptos = root.findall('.//cfdi:Concepto', ns)
            productos = []

            for concepto in conceptos[:5]:  # Máximo 5 productos
                descripcion = concepto.get('Descripcion', '')
                if descripcion and self.es_producto_valido(descripcion):
                    productos.append(descripcion)

            if productos:
                resultado['descripcion'] = ", ".join(productos[:3])
                print(f"      📦 Productos del XML: {resultado['descripcion'][:80]}...")

            return resultado

        except Exception as e:
            print(f"      ❌ Error procesando XML: {str(e)[:50]}")
            return {
                'descripcion': "Error al procesar XML",
                'folio_fiscal': "Error al procesar XML"
            }

    def extraer_datos_factura(self, url, comercio=None, fecha=None, total=None):
        """
        Extrae datos de RindeGastos con manejo mejorado y búsqueda local como fallback
        """
        try:
            print(f"🔍 Accediendo a: {url}")

            # Obtener página
            response = self.session.get(url, headers=self.headers, timeout=20)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            resultado = {
                'descripcion': "No encontrada",
                'folio_fiscal': "No encontrado"
            }

            # Buscar enlaces PDF mejorado
            enlaces_pdf = self.buscar_enlaces_pdf_mejorado(soup, url)

            # Si no hay enlaces directos, construir URLs
            if not enlaces_pdf:
                enlaces_pdf = self.construir_urls_descarga(url)

            # Procesar PDFs
            pdf_procesado = False

            for enlace in enlaces_pdf:
                pdf_content = self.descargar_pdf(enlace, url)

                if pdf_content:
                    resultado = self.procesar_pdf_mejorado(pdf_content)
                    pdf_procesado = True
                    break

            # Si no se pudo procesar PDF, intentar HTML
            if not pdf_procesado:
                print("   ⚠️ No se pudo procesar PDF, extrayendo del HTML...")
                texto_html = soup.get_text()
                resultado = self.procesar_texto_factura_mejorado(texto_html)

            # Si no encontramos nada útil, buscar en carpeta local
            if (resultado['descripcion'] in ["No encontrada", "Númerodepedimento", "P. Unitario"] or
                    resultado['folio_fiscal'] == "No encontrado"):

                if self.carpeta_cfdi and comercio and total:
                    print("   📂 Buscando en carpeta local de XMLs...")
                    xml_local = self.buscar_xml_local(comercio, fecha, total)

                    if xml_local:
                        print(f"   ✅ XML encontrado localmente: {xml_local.name}")
                        resultado_xml = self.procesar_xml_cfdi(xml_local)

                        # Actualizar solo si encontramos mejores datos
                        if resultado_xml['descripcion'] != "No encontrada":
                            resultado['descripcion'] = resultado_xml['descripcion'] + " (XML local)"
                        if resultado_xml['folio_fiscal'] != "No encontrado":
                            resultado['folio_fiscal'] = resultado_xml['folio_fiscal']

            return resultado

        except Exception as e:
            print(f"   ❌ Error general: {str(e)[:100]}")

            # Intentar búsqueda local como último recurso
            if self.carpeta_cfdi and comercio and total:
                print("   📂 Intentando búsqueda local como último recurso...")
                xml_local = self.buscar_xml_local(comercio, fecha, total)

                if xml_local:
                    print(f"   ✅ XML encontrado localmente: {xml_local.name}")
                    return self.procesar_xml_cfdi(xml_local)

            return {
                'descripcion': f"Error: {str(e)[:50]}",
                'folio_fiscal': f"Error: {str(e)[:50]}"
            }

    def buscar_enlaces_pdf_mejorado(self, soup, url_original):
        """
        Busca enlaces a PDFs con estrategia mejorada
        """
        enlaces_pdf = []

        # Buscar en todos los enlaces
        for link in soup.find_all('a'):
            href = link.get('href', '')
            texto = link.get_text(strip=True).lower()

            # Condiciones mejoradas
            if href and (
                    '.pdf' in href.lower() or
                    's3.amazonaws.com' in href or
                    'ppstatic' in href or
                    ('descargar' in texto or 'download' in texto)
            ):
                if href.startswith('http'):
                    enlaces_pdf.append(href)
                    print(f"   🎯 PDF encontrado: {href[:60]}...")
                elif href.startswith('/'):
                    # URL relativa
                    base_url = '/'.join(url_original.split('/')[:3])
                    enlace_completo = base_url + href
                    enlaces_pdf.append(enlace_completo)

        # Buscar en iframes
        for iframe in soup.find_all('iframe'):
            src = iframe.get('src', '')

            if src and ('pdf' in src.lower() or 'viewer' in src.lower()):
                # Extraer URL del PDF del parámetro file
                if 'file=' in src:
                    try:
                        parsed = urllib.parse.urlparse(src)
                        params = urllib.parse.parse_qs(parsed.query)
                        if 'file' in params:
                            pdf_url = urllib.parse.unquote(params['file'][0])
                            enlaces_pdf.append(pdf_url)
                            print(f"   🎯 PDF en iframe: {pdf_url[:60]}...")
                    except:
                        pass

        # Buscar en el texto/scripts de la página
        page_text = str(soup)
        # Buscar URLs de S3 o PDFs en el código
        s3_pattern = r'https?://[^\s\'"]+\.s3\.amazonaws\.com/[^\s\'"]+\.pdf'
        pdf_pattern = r'https?://[^\s\'"]+\.pdf'

        for pattern in [s3_pattern, pdf_pattern]:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            for match in matches:
                if match not in enlaces_pdf:
                    enlaces_pdf.append(match)
                    print(f"   🎯 PDF en código: {match[:60]}...")

        return enlaces_pdf

    def construir_urls_descarga(self, url):
        """
        Construye URLs de descarga posibles
        """
        enlaces = []

        receipt_match = re.search(r'i=(\d+)', url)
        key_match = re.search(r'key=([^&]+)', url)

        if receipt_match and key_match:
            receipt_id = receipt_match.group(1)
            key = key_match.group(1)

            # URLs comunes
            enlaces.extend([
                f"{url}&download=1",
                f"{url}&format=pdf",
                f"{url}&tipo=pdf",
                f"https://www.rindegastos.com/document/download/{receipt_id}?key={key}",
                f"https://web.rindegastos.com/document/download/{receipt_id}?key={key}",
                url.replace('/receipt', '/download')
            ])

        return enlaces

    def descargar_pdf(self, url_pdf, referer):
        """
        Descarga el PDF con reintentos
        """
        for intento in range(3):
            try:
                print(f"   📥 Descargando (intento {intento + 1}): {url_pdf[:80]}...")

                pdf_headers = self.headers.copy()
                pdf_headers.update({
                    'Accept': 'application/pdf,application/octet-stream,*/*',
                    'Referer': referer
                })

                response = self.session.get(url_pdf, headers=pdf_headers, timeout=30, stream=True)

                if response.status_code == 200:
                    content = response.content

                    # Verificar que sea PDF
                    if len(content) > 1000 and (
                            content.startswith(b'%PDF') or
                            b'%PDF' in content[:1024]
                    ):
                        print(f"   ✅ PDF descargado: {len(content):,} bytes")
                        return content
                    else:
                        print(f"   ⚠️ No es un PDF válido")
                else:
                    print(f"   ❌ Error HTTP: {response.status_code}")

            except Exception as e:
                print(f"   ❌ Error descarga: {str(e)[:50]}")

            if intento < 2:
                time.sleep(1)

        return None

    def procesar_pdf_mejorado(self, pdf_content):
        """
        Procesa el PDF con estrategias mejoradas
        """
        resultado = {
            'descripcion': "No encontrada",
            'folio_fiscal': "No encontrado"
        }

        try:
            with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
                texto_completo = ""
                productos_encontrados = []

                for page_num, page in enumerate(pdf.pages):
                    print(f"   📄 Procesando página {page_num + 1}")

                    # Extraer texto
                    texto = page.extract_text()
                    if texto:
                        texto_completo += texto + "\n"

                    # Extraer y procesar tablas
                    tablas = page.extract_tables()
                    if tablas:
                        for tabla in tablas:
                            productos_tabla = self.extraer_productos_de_tabla(tabla)
                            productos_encontrados.extend(productos_tabla)

                # Buscar folio fiscal
                resultado['folio_fiscal'] = self.extraer_folio_fiscal(texto_completo)

                # Si encontramos productos en las tablas
                if productos_encontrados:
                    # Filtrar productos válidos
                    productos_validos = []
                    for prod in productos_encontrados:
                        if self.es_producto_valido(prod):
                            productos_validos.append(prod)

                    if productos_validos:
                        # Tomar hasta 3 productos
                        resultado['descripcion'] = ", ".join(productos_validos[:3])
                        print(f"   ✅ Productos encontrados: {len(productos_validos)}")

                # Si no hay productos en tablas, buscar en texto
                if resultado['descripcion'] == "No encontrada":
                    productos_texto = self.buscar_productos_en_texto_mejorado(texto_completo)
                    if productos_texto:
                        resultado['descripcion'] = ", ".join(productos_texto[:3])

        except Exception as e:
            print(f"   ⚠️ Error procesando PDF: {e}")

            # Intentar con PyPDF2
            try:
                reader = PyPDF2.PdfReader(io.BytesIO(pdf_content))
                texto_completo = ""

                for page in reader.pages:
                    texto_completo += page.extract_text() + "\n"

                if texto_completo:
                    resultado = self.procesar_texto_factura_mejorado(texto_completo)

            except Exception as e2:
                print(f"   ❌ Error con PyPDF2: {e2}")

        return resultado

    def extraer_productos_de_tabla(self, tabla):
        """
        Extrae productos de una tabla específica
        """
        productos = []

        if not tabla or len(tabla) < 2:
            return productos

        # Buscar columna de descripción
        encabezados = tabla[0]
        indice_descripcion = -1

        for i, header in enumerate(encabezados):
            if header and any(palabra in str(header).upper() for palabra in
                              ['DESCRIPCIÓN', 'DESCRIPCION', 'CONCEPTO', 'NOMBRE', 'PRODUCTO']):
                indice_descripcion = i
                break

        # Si encontramos la columna, extraer productos
        if indice_descripcion >= 0:
            for fila in tabla[1:]:  # Saltar encabezados
                if len(fila) > indice_descripcion and fila[indice_descripcion]:
                    producto = str(fila[indice_descripcion]).strip()

                    # Verificar que no sea un valor numérico o vacío
                    if producto and not re.match(r'^[\d\.\,\$\s\-]+$', producto):
                        productos.append(producto)

        return productos

    def es_producto_valido(self, texto):
        """
        Verifica si el texto es un producto válido
        """
        texto_upper = texto.upper()

        # Rechazar si contiene palabras excluidas
        for palabra in self.palabras_excluir:
            if palabra in texto_upper:
                return False

        # Rechazar si es muy corto o muy largo
        if len(texto) < 10 or len(texto) > 300:
            return False

        # Rechazar si es solo números/símbolos
        if re.match(r'^[\d\.\,\$\s\-\%\/]+$', texto):
            return False

        # Aceptar si contiene palabras de productos conocidos
        palabras_producto = [
            'SOPORTE', 'LED', 'LAMPARA', 'FOCO', 'FELPA', 'SOLDADURA',
            'LIJA', 'TORNILLO', 'TUBO', 'ADHESIVO', 'BROCHA', 'PINTURA',
            'CEMENTO', 'CABLE', 'MOTOR', 'CONTROL', 'GAUGE', 'SENSOR',
            'TERMOSTATO', 'TERMOPILA', 'VALVULA', 'LLAVE', 'CODO',
            'BOQUILLA', 'PANEL', 'EXTRACTOR', 'CINTA', 'SILICON'
        ]

        for palabra in palabras_producto:
            if palabra in texto_upper:
                return True

        # Aceptar si parece una descripción (tiene varias palabras)
        palabras = texto.split()
        if len(palabras) >= 3:
            return True

        return False

    def buscar_productos_en_texto_mejorado(self, texto):
        """
        Busca productos en el texto con filtros mejorados
        """
        productos = []
        lineas = texto.split('\n')

        # Buscar después de palabras clave
        en_seccion_productos = False

        for i, linea in enumerate(lineas):
            linea_limpia = linea.strip()
            linea_upper = linea_limpia.upper()

            # Detectar sección de productos
            if any(palabra in linea_upper for palabra in
                   ['DESCRIPCIÓN', 'CONCEPTO', 'PRODUCTO', 'ARTÍCULO']):
                en_seccion_productos = True
                continue

            # Detectar fin de sección
            if en_seccion_productos and any(palabra in linea_upper for palabra in
                                            ['SUBTOTAL', 'TOTAL', 'DESCUENTO', 'IMPUESTO']):
                break

            # Si estamos en productos y la línea es válida
            if en_seccion_productos and self.es_producto_valido(linea_limpia):
                productos.append(linea_limpia)

                # Máximo 5 productos para no saturar
                if len(productos) >= 5:
                    break

        return productos

    def extraer_folio_fiscal(self, texto):
        """
        Extrae el folio fiscal/UUID
        """
        # Patrones para UUID
        patrones = [
            r'([A-F0-9]{8}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{12})',
            r'(?:Folio\s*Fiscal|UUID|TimbreFiscal)[:\s]*([A-F0-9-]{36})',
            r'(?:No\.\s*de\s*Serie\s*del\s*Certificado\s*del\s*SAT)[:\s]*([0-9]{20})'
        ]

        for patron in patrones:
            matches = re.findall(patron, texto, re.IGNORECASE)
            if matches:
                return matches[0].upper()

        return "No encontrado"

    def procesar_texto_factura_mejorado(self, texto):
        """
        Procesa texto con todas las mejoras
        """
        resultado = {
            'descripcion': "No encontrada",
            'folio_fiscal': self.extraer_folio_fiscal(texto)
        }

        # Buscar productos
        productos = self.buscar_productos_en_texto_mejorado(texto)
        if productos:
            resultado['descripcion'] = ", ".join(productos[:3])

        return resultado

    def procesar_excel(self, archivo_entrada, archivo_salida):
        """
        Procesa el archivo Excel
        """
        print("\n" + "=" * 80)
        print("🚀 EXTRACTOR DE FACTURAS RINDEGASTOS V7 - CON BÚSQUEDA LOCAL")
        print("=" * 80)

        # Cargar Excel
        print("📊 Cargando archivo Excel...")
        df = pd.read_excel(archivo_entrada)

        # Filtrar solo facturas
        print("🔍 Filtrando solo facturas...")
        df_facturas = df[df['Tipo de documento'] == 'Factura'].copy()
        print(f"📋 Total registros: {len(df)}")
        print(f"📋 Facturas encontradas: {len(df_facturas)}")

        if len(df_facturas) == 0:
            print("❌ No se encontraron facturas")
            return

        # Resetear índice
        df_facturas.reset_index(drop=True, inplace=True)

        # Inicializar columnas
        if 'Descripción' not in df_facturas.columns:
            df_facturas['Descripción'] = ''
        if 'Folio Fiscal Extraído' not in df_facturas.columns:
            df_facturas['Folio Fiscal Extraído'] = ''
        if 'Fuente' not in df_facturas.columns:
            df_facturas['Fuente'] = ''

        # Estadísticas
        exitosas_desc = 0
        exitosas_folio = 0
        desde_xml_local = 0
        errores = 0
        tiempo_inicio = time.time()

        # Procesar cada factura
        print(f"\n{'=' * 80}")
        print("📦 PROCESANDO FACTURAS")
        if self.carpeta_cfdi:
            print(f"📂 Carpeta XMLs: {self.carpeta_cfdi}")
        print(f"{'=' * 80}\n")

        for idx in range(len(df_facturas)):
            fila = df_facturas.iloc[idx]
            url = fila['URL']
            comercio = fila['Comercio']
            total = fila['Total']
            fecha = fila['Fecha']

            if pd.isna(url) or not url:
                continue

            print(f"\n{'─' * 70}")
            print(f"📄 Factura {idx + 1}/{len(df_facturas)}")
            print(f"🏪 Comercio: {comercio}")
            print(f"💰 Total: ${total:,.2f}")
            print(f"📅 Fecha: {fecha}")

            # Procesar factura
            resultado = self.extraer_datos_factura(url, comercio, fecha, total)

            # Guardar resultados
            df_facturas.at[idx, 'Descripción'] = resultado['descripcion']
            df_facturas.at[idx, 'Folio Fiscal Extraído'] = resultado['folio_fiscal']

            # Marcar fuente
            if "(XML local)" in resultado['descripcion']:
                df_facturas.at[idx, 'Fuente'] = "XML Local"
                desde_xml_local += 1
            else:
                df_facturas.at[idx, 'Fuente'] = "Web"

            # Verificar si la descripción es válida (no es encabezado)
            if resultado['descripcion'] not in ['No encontrada', 'Númerodepedimento', 'P. Unitario']:
                exitosas_desc += 1
                print(f"   ✅ Descripción: {resultado['descripcion'][:80]}...")
            else:
                print(f"   ❌ Descripción: {resultado['descripcion']}")

            if resultado['folio_fiscal'] != "No encontrado" and "Error" not in resultado['folio_fiscal']:
                exitosas_folio += 1
                print(f"   ✅ Folio: {resultado['folio_fiscal']}")
            else:
                print(f"   ❌ Folio: {resultado['folio_fiscal']}")

            if "Error" in resultado['descripcion'] or "Error" in resultado['folio_fiscal']:
                errores += 1

            # Progreso cada 5 facturas
            if (idx + 1) % 5 == 0:
                print(f"\n{'=' * 50}")
                print(f"📊 PROGRESO: {idx + 1}/{len(df_facturas)} ({(idx + 1) / len(df_facturas) * 100:.1f}%)")
                print(f"   ✅ Descripciones válidas: {exitosas_desc}/{idx + 1} ({exitosas_desc / (idx + 1) * 100:.1f}%)")
                print(f"   ✅ Folios: {exitosas_folio}/{idx + 1} ({exitosas_folio / (idx + 1) * 100:.1f}%)")
                print(f"   📂 Desde XML local: {desde_xml_local}")
                print(f"{'=' * 50}\n")

            # Pausa entre requests
            time.sleep(2)

        # Guardar resultados
        print(f"\n💾 Guardando resultados...")
        df_facturas.to_excel(archivo_salida, index=False)

        # Resumen final
        tiempo_total = time.time() - tiempo_inicio

        print(f"\n{'=' * 80}")
        print(f"✅ PROCESAMIENTO COMPLETADO")
        print(f"{'=' * 80}")
        print(f"\n📊 RESUMEN FINAL:")
        print(f"   📁 Archivo guardado: {archivo_salida}")
        print(f"   📋 Total facturas procesadas: {len(df_facturas)}")
        print(f"   ✅ Descripciones válidas: {exitosas_desc} ({exitosas_desc / len(df_facturas) * 100:.1f}%)")
        print(f"   ✅ Folios extraídos: {exitosas_folio} ({exitosas_folio / len(df_facturas) * 100:.1f}%)")
        print(f"   📂 Encontradas en XML local: {desde_xml_local}")
        print(f"   ❌ Errores: {errores}")
        print(f"   ⏱️ Tiempo total: {tiempo_total / 60:.1f} minutos")

        # Mostrar facturas problemáticas
        print(f"\n📋 FACTURAS SIN DESCRIPCIÓN VÁLIDA:")
        sin_desc = df_facturas[
            (df_facturas['Descripción'] == 'No encontrada') |
            (df_facturas['Descripción'] == 'Númerodepedimento') |
            (df_facturas['Descripción'] == 'P. Unitario')
            ]

        if len(sin_desc) > 0:
            print(f"   Total: {len(sin_desc)} facturas")
            for _, fila in sin_desc.head(10).iterrows():
                print(f"   - {fila['Comercio']}: {fila['URL'][:60]}...")

        # Mostrar estadísticas de XML local
        if desde_xml_local > 0:
            print(f"\n📂 FACTURAS RECUPERADAS DESDE XML LOCAL:")
            facturas_xml = df_facturas[df_facturas['Fuente'] == 'XML Local']
            for _, fila in facturas_xml.iterrows():
                print(f"   - {fila['Comercio']}: ${fila['Total']:,.2f}")


# ========== PROGRAMA PRINCIPAL ==========
if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("🚀 EXTRACTOR DE FACTURAS RINDEGASTOS V7 - CON BÚSQUEDA LOCAL")
    print("   • Búsqueda en carpeta local de XMLs como fallback")
    print("   • Mejor detección de PDFs en S3")
    print("   • Filtros mejorados contra encabezados")
    print("   • Validación estricta de productos")
    print("=" * 80)

    # Verificar dependencias
    try:
        import pdfplumber

        print("✅ pdfplumber disponible")
    except ImportError:
        print("❌ pdfplumber requerido - instalar con: pip install pdfplumber")
        exit(1)

    # Configuración
    archivo_entrada = "/Users/gbphy/Downloads/2025_ago_4_Gastos-2.xlsx"
    archivo_salida = "/Users/gbphy/Downloads/2025_ago_4_Gastos_VFinal.xlsx"
    carpeta_cfdi = "/Users/gbphy/Downloads/CFDI Junio 2025"

    print(f"\n📂 Archivo entrada: {archivo_entrada}")
    print(f"📤 Archivo salida: {archivo_salida}")
    print(f"📁 Carpeta XMLs: {carpeta_cfdi}")

    # Verificar archivo y carpeta
    import os

    if not os.path.exists(archivo_entrada):
        print(f"\n❌ No se encontró el archivo: {archivo_entrada}")
        exit(1)

    if not os.path.exists(carpeta_cfdi):
        print(f"\n⚠️ Advertencia: No se encontró la carpeta de XMLs: {carpeta_cfdi}")
        print("   Continuará sin búsqueda local de XMLs")
        respuesta = input("\n¿Continuar sin carpeta de XMLs? (s/n): ")
        if respuesta.lower() != 's':
            exit(1)
        carpeta_cfdi = None

    # Confirmar
    respuesta = input("\n¿Iniciar procesamiento? (s/n): ")

    if respuesta.lower() == 's':
        extractor = ExtractorFacturasRindeGastosV7(carpeta_cfdi=carpeta_cfdi)

        try:
            extractor.procesar_excel(archivo_entrada, archivo_salida)
            print("\n✅ ¡Proceso completado!")

        except KeyboardInterrupt:
            print("\n\n⚠️ Proceso interrumpido")
        except Exception as e:
            print(f"\n\n❌ Error crítico: {e}")
            import traceback

            traceback.print_exc()
    else:
        print("\n❌ Proceso cancelado")