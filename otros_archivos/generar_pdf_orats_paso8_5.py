import os
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, ListItem, ListFlowable
from reportlab.lib.colors import HexColor

def generar_pdf_paso8_5_actualizado(output_path):
    doc = SimpleDocTemplate(output_path, pagesize=letter,
                            rightMargin=50, leftMargin=50,
                            topMargin=50, bottomMargin=50)

    styles = getSampleStyleSheet()
    
    # Custom styles
    title_style = ParagraphStyle(
        'TitleStyle',
        parent=styles['Heading1'],
        fontSize=20,
        textColor=HexColor("#1e3a8a"),
        spaceAfter=20,
        alignment=1 # Center
    )
    
    heading_style = ParagraphStyle(
        'HeadingStyle',
        parent=styles['Heading2'],
        fontSize=14,
        textColor=HexColor("#2563eb"),
        spaceBefore=15,
        spaceAfter=10
    )
    
    body_style = ParagraphStyle(
        'BodyStyle',
        parent=styles['Normal'],
        fontSize=11,
        leading=15,
        spaceAfter=15
    )
    
    bullet_style = ParagraphStyle(
        'BulletStyle',
        parent=styles['Normal'],
        fontSize=11,
        leading=14,
        spaceAfter=5
    )

    story = []

    # Title
    story.append(Paragraph("Documentación Técnica Pipeline", title_style))
    story.append(Paragraph("Paso 8.5: Enriquecimiento Histórico FMP y Top 3000 Dinámico", title_style))
    story.append(Spacer(1, 10))

    # Introducción
    story.append(Paragraph("1. Objetivo Principal", heading_style))
    intro_text = """
    El Paso 8.5 (`fmp_ticker_updater` via `run_pipeline.py`) tiene la misión crítica de nutrir el sistema con los precios OHLCV (Open, High, Low, Close, Volume) diarios e históricos provenientes de la API de Financial Modeling Prep (FMP). 
    <br/><br/>
    En su versión actualizada, la ejecución de este paso está diseñada de forma dual: protege y mantiene al día tu universo específico central de estudio, pero al mismo tiempo abre las puertas a toda la inmensidad del mercado extrayendo de forma algorítmica a los 3000 gigantes más líquidos.
    """
    story.append(Paragraph(intro_text, body_style))

    # Fase A y B
    story.append(Paragraph("2. Ejecución Dual Secuencial", heading_style))
    
    dual_desc = "El código corre internamente dos rutinas principales:"
    story.append(Paragraph(dual_desc, body_style))

    fases_ejecucion = [
        ListItem(Paragraph("<b>PASO 8.5 (A) - El Universo Base Constante:</b> El programa lee silenciosamente tu `universo.json` (aproximadamente 256 tickers clásicos de ORATS) y se asegura de descargar/actualizar todos los precios históricos de **forma cien por ciento incremental** en un archivo enfocado: `fmp_prices.parquet`.", bullet_style)),
        ListItem(Paragraph("<b>PASO 8.5 (B) - La Exploración Top 3000 Dinámica:</b> A continuación, en fracciones de segundo y de cero, el sistema escanea los archivos diarios maestros (Cores de ORATS) en la carpeta `datos_cores`. Suma los volúmenes transaccionales reportados a nivel mercado global y extrae a los 3000 Stocks/ETFs más dominantes para descargar u actualizar su historia desde 2019 directo hacia el archivo mayor `fmp_prices_top3000.parquet`.", bullet_style))
    ]
    story.append(ListFlowable(fases_ejecucion, bulletType='bullet', start='circle'))
    story.append(Spacer(1, 10))

    # Mecánicas de Descarga
    story.append(Paragraph("3. Eficiencia: Incrementalidad y Multithreading", heading_style))
    mecanicas_text = """
    Resultaría inviable redescargar a diario millones de filas de FMP desde 2019. Por ello, el corazón lógico de la clase `FMPDailyDownloader` incluye una <b>inteligencia de mapeo de fechas (Gaps)</b>:
    """
    story.append(Paragraph(mecanicas_text, body_style))

    fases_tecnicas = [
        ListItem(Paragraph("<b>Detección de Archivo:</b> Si el fichero `.parquet` destino ya existe, el script lo lee e identifica de forma asíncrona la fecha máxima (fecha del último precio disponible) para cada uno de los tickers.", bullet_style)),
        ListItem(Paragraph("<b>Solicitud Parcial:</b> A continuación, prepara un Pool de Conexiones (Thread Pool Executor) para solicitarle a FMP a través de HTTPS <i>únicamente los días faltantes</i> entre esa última fecha máxima y el día actual para cada ticker individualmente.", bullet_style)),
        ListItem(Paragraph("<b>Velocidad Constante:</b> Gracias a este diseño, mientras que la ejecución original (día 0) requerirá bajar más de 5 años de datos, todas las ejecuciones al día siguiente del Paso 8.5 actualizarán paralelamente el mercado global top y local en contados segundos.", bullet_style))
    ]
    story.append(ListFlowable(fases_tecnicas, bulletType='bullet', start='circle'))
    story.append(Spacer(1, 10))

    # Salida
    story.append(Paragraph("4. Salidas Generadas (Outputs)", heading_style))
    output_text = """
    Toda la matriz de precios, con cotizaciones estrictamente ajustadas por _splits_ y dividendos para asegurar pulcritud y continuidad matemática, se guarda comprimida mediante Snappy en:<br/>
    <ul>
     <li>`C:\\datos_proyecto\\datos_stocks\\fmp_prices.parquet` (Base)</li>
     <li>`C:\\datos_proyecto\\datos_stocks\\fmp_prices_top3000.parquet` (Caza de Liquidez)</li>
    </ul>
    """
    story.append(Paragraph(output_text, body_style))

    # Build PDF
    doc.build(story)
    print(f"PDF generado con éxito en: {output_path}")

if __name__ == "__main__":
    output_pdf = r"c:\proyecto\otros_archivos\Paso_8_5_Explicacion_Detallada_v2.pdf"
    
    # Crear carpeta si no existe
    os.makedirs(os.path.dirname(output_pdf), exist_ok=True)
    
    # Generar
    generar_pdf_paso8_5_actualizado(output_pdf)
