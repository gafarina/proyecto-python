import os
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.platypus.flowables import KeepTogether

def create_pdf(filename="explicacion_orats_paso5.pdf"):
    # 1. Configurar el lienzo base
    doc = SimpleDocTemplate(
        filename,
        pagesize=A4,
        rightMargin=1 * inch,
        leftMargin=1 * inch,
        topMargin=1 * inch,
        bottomMargin=1 * inch
    )

    # 2. Hojas de Estilos
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle(
        name='MainTitle',
        parent=styles['Title'],
        fontName='Helvetica-Bold',
        fontSize=24,
        leading=28,
        textColor=colors.HexColor("#2A4365"),
        spaceAfter=20,
        alignment=1
    )

    intro_style = ParagraphStyle(
        name='Intro',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=12,
        leading=18,
        textColor=colors.HexColor("#4A5568"),
        spaceAfter=20
    )

    step_title_style = ParagraphStyle(
        name='StepTitle',
        parent=styles['Heading2'],
        fontName='Helvetica-Bold',
        fontSize=14,
        leading=18,
        textColor=colors.HexColor("#2B6CB0"),
        spaceBefore=15,
        spaceAfter=8
    )

    body_style = ParagraphStyle(
        name='Body',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=11,
        leading=16,
        textColor=colors.HexColor("#2D3748"),
        spaceAfter=8
    )

    code_style = ParagraphStyle(
        name='CodeStyle',
        parent=styles['Normal'],
        fontName='Courier',
        fontSize=10,
        leading=14,
        textColor=colors.HexColor("#E2E8F0"),
        backColor=colors.HexColor("#1A202C"),
        borderPadding=(8, 10, 8, 10),
        borderWidth=1,
        borderColor=colors.HexColor("#2D3748"),
        borderRadius=5,
        spaceBefore=5,
        spaceAfter=15
    )

    story = []

    # 3. Encabezado y Títulos
    story.append(Paragraph("Análisis de Código run_pipeline", ParagraphStyle(
        name='Sup', fontName='Helvetica-Bold', fontSize=10, textColor=colors.HexColor("#A0AEC0"), alignment=1, spaceAfter=5
    )))
    story.append(Paragraph("Explicación del Paso 5: Descarga Maestra de Earnings", title_style))
    story.append(Spacer(1, 0.2 * inch))

    intro_text = (
        "El <b>Paso 5</b> abandona por un momento a ORATS y se conecta con Benzinga (un proveedor de noticias institucionales). "
        "Su objetivo es crear y mantener un calendario maestro ultra-preciso de todos los reportes de ganancias (Earnings Dates) de "
        "todas las empresas, pasadas y futuras. Esto es sumamente letal y fundamental para el modelaje cuantitativo de riesgo más adelante."
    )
    story.append(Paragraph(intro_text, intro_style))

    # 4. Datos Estructurados de la Explicación
    steps_data = [
        {
            "title": "A. ¿Por qué Benzinga y no ORATS?",
            "body": "El código usa una importación especial llamada <font name='Courier' color='#D53F8C'>massive (RESTClient)</font>. "
                    "Inyecta en duro una API Key distinta (<font name='Courier' color='#D53F8C'>JTm_3R45Mw...YERgkBT</font>) en lugar de "
                    "la de ORATS. Benzinga es famoso en Wall Street por entregar no solo el <i>día</i> del reporte, sino la "
                    "<i>hora exacta</i> (Antes de la apertura o Después del cierre), lo cual afecta gigantescamente a las opciones.",
            "code": "def fetch_batch_worker(self, tickers_batch, date_from):\n    BENZINGA_KEY = 'JTm_3R45MwCC6fDqfG7fVDmw0YERgkBT'\n    client = RESTClient(BENZINGA_KEY)\n    api_data = client.list_benzinga_earnings(ticker_any_of=tickers_batch, date_gte=date_from)"
        },
        {
            "title": "B. Rastreo de tu Universo Activo (El radar)",
            "body": "No escanea los 10,000 tickers del mercado. Invoca la función interna <font name='Courier' color='#D53F8C'>_get_universe_from_wheels()</font>, "
                    "la cual busca furtivamente dentro de tus últimos 5 archivos `.parquet` de opciones descargados qué Tickers están activos *hoy*. "
                    "Alimenta esa lista dinámica al rastreador de Benzinga para no malgastar peticiones en empresas muertas.",
            "code": "def _get_universe_from_wheels(self):\n    files = glob.glob(os.path.join(self.ruedas_dir, 'rueda_*.parquet'))\n    recent_files = files[-5:] # Últimos 5 días\n    # ... recolecta tickers de estos archivos ..."
        },
        {
            "title": "C. Estrategia Incremental Bifurcada",
            "body": "El script hace algo muy inteligente al descargar la historia. Compara tu universo detectado con la base de datos "
                    "<font name='Courier' color='#D53F8C'>universe_earnings.parquet</font> (si es que ya existe en tu disco duro). "
                    "Genera dos grupos radicalmente distintos de trabajo:<br/><br/>"
                    "1. <b>Para Tickers Viejos</b>: Retrocede apenas 45 días al pasado para parchear fechas, y descarga el futuro.<br/>"
                    "2. <b>Para Tickers Nuevos</b>: Si una empresa nueva hizo un IPO (Salió a la bolsa) o acaba de entrar a tu Top-Volume, "
                    "el script exige a Benzinga su historial compelto absoluto arrancando desde <font name='Courier' color='#D53F8C'>2019-01-01</font>.",
            "code": "if os.path.exists(self.master_earnings_path):\n    # ... (lectura inteligente) ...\n    to_download_recent = [t for t in target_universe if t in existing_tickers]\n    to_download_full = [t for t in target_universe if t not in existing_tickers]"
        },
        {
            "title": "D. Ingeniería de Diccionarios y Recolección de EPS",
            "body": "La función `fetch_batch_worker` desarma la respuesta que envía Benzinga. No se conforma solo con fechas. "
                    "Secuestra atributos financieros vitales como <font name='Courier' color='#D53F8C'>eps_estimate</font> (lo que Wall Street cree que ganarán), "
                    "<font name='Courier' color='#D53F8C'>eps_actual</font> (lo que realmente ganaron) y el <font name='Courier' color='#D53F8C'>revenue_actual</font> "
                    "(Ingresos netos). Y lo empaqueta todo en una limpia matriz estructurada.",
            "code": "record = {\n    'ticker': getattr(e, 'ticker', None),\n    'date': getattr(e, 'date', None),\n    'time': getattr(e, 'time', None),\n    'eps_estimate': getattr(e, 'eps_estimate', None),\n    'eps_actual': getattr(e, 'actual_eps', None)\n}"
        },
        {
            "title": "E. Consolidación Parquet (El Anti-Duplicado)",
            "body": "Con todos los paquetes nuevos de información, el código invoca el poder de Polars usando la técnica de 'Upsert'. "
                    "En vez de duplicar eventos de ganancias si los descargaste ayer y hoy, utiliza la función "
                    "<font name='Courier' color='#D53F8C'>.unique(subset=['ticker', 'date'], keep='last')</font>. Conserva siempre "
                    "la estimación más actualizada de los analistas, sobreescribiendo limpiamente tu Archi-Maestro.",
            "code": "combined = pl.concat([old_df, new_df], how='diagonal')\n# Elimina duplicados manteniendo la versión más moderna\ncombined = combined.unique(subset=['ticker', 'date'], keep='last')\ncombined.write_parquet(self.master_earnings_path)"
        }
    ]

    for step in steps_data:
        step_flowables = [
            Paragraph(step["title"], step_title_style),
            Paragraph(step["body"], body_style)
        ]
        
        code_text = step["code"].replace('\n', '<br/>').replace(' ', '&nbsp;')
        step_flowables.append(Paragraph(code_text, code_style))
        
        story.append(KeepTogether(step_flowables))
        story.append(Spacer(1, 10))

    # Pie de página
    story.append(Spacer(1, 30))
    footer_style = ParagraphStyle(
        name='Footer',
        parent=styles['Normal'],
        fontName='Helvetica-Oblique',
        fontSize=9,
        textColor=colors.HexColor("#A0AEC0"),
        alignment=1
    )
    story.append(Paragraph("Reporte Modular: Explicación de Código - Generado de forma automatizada", footer_style))

    doc.build(story)
    print(f"PDF generado exitosamente: {os.path.abspath(filename)}")

if __name__ == "__main__":
    create_pdf()
