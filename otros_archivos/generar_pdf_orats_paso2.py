import os
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.platypus.flowables import KeepTogether

def create_pdf(filename="explicacion_orats_paso2.pdf"):
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
    story.append(Paragraph("Análisis de Código OratsDataManager", ParagraphStyle(
        name='Sup', fontName='Helvetica-Bold', fontSize=10, textColor=colors.HexColor("#A0AEC0"), alignment=1, spaceAfter=5
    )))
    story.append(Paragraph("Explicación del Paso 2: Verificación de Integridad", title_style))
    story.append(Spacer(1, 0.2 * inch))

    intro_text = (
        "El <b>Paso 2</b> de tu script <font name='Courier' color='#D53F8C'>orats_data_manager.py</font> es crucial para la higiene de tus datos. "
        "A través del método <font name='Courier' color='#D53F8C'>check_integrity</font>, el sistema actúa como un auditor interno. Su propósito "
        "es doble: asegurarse de que no falte ningún archivo de la historia registrada, y garantizar que los archivos que sí existen no estén corruptos o vacíos."
    )
    story.append(Paragraph(intro_text, intro_style))

    # 4. Datos Estructurados de la Explicación
    steps_data = [
        {
            "title": "A. Auditoría de Calendario (trading_days vs existing_days)",
            "body": "El sistema extrae dos listas inmensas de fechas. Una lista teórica (<font name='Courier' color='#D53F8C'>trading_days</font>) "
                    "que representa cada uno de los días en los que el mercado estuvo abierto desde el año de inicio. Y otra lista real "
                    "(<font name='Courier' color='#D53F8C'>existing_days</font>) que representa los archivos que tienes actualmente en la carpeta "
                    "<font name='Courier' color='#D53F8C'>datos_cores</font>.",
            "code": "trading_days = set(self.get_trading_days(start_date, end_date))\nexisting_days = self.get_existing_dates()"
        },
        {
            "title": "B. Detección de Huecos o Gaps (Matemática de Conjuntos)",
            "body": "Como ambas listas se guardan en estructuras de 'Conjuntos' (Set), Python puede calcular rápidamente la diferencia matemática entre ellas usando el "
                    "operador de resta (<font name='Courier' color='#D53F8C'>-</font>). Esto encuentra instantáneamente si hay alguna fecha que el "
                    "calendario diga que debió existir, pero que no está en tu computadora. En caso de detectarlo, imprime una alerta.",
            "code": "# Calcula la diferencia de conjuntos (A - B)\nmissing = sorted(list(trading_days - existing_days))\nif missing:\n    print(f'[WARN] Faltan {len(missing)} días hábiles...')"
        },
        {
            "title": "C. Escaneo de Corrupción de Archivos (Glob)",
            "body": "No basta con que el archivo exista; debe contener datos reales. La librería <font name='Courier' color='#D53F8C'>glob</font> busca "
                    "todos los archivos <i>.parquet</i> de nuevo. Al iterar sobre ellos, no necesita abrirlos y leerlos por dentro (lo cual "
                    "sería excesivamente pesado si tuvieras miles de días). En su lugar, el sistema le pregunta al Sistema Operativo Windows "
                    "físicamente cuánto pesa el archivo usando <font name='Courier' color='#D53F8C'>os.path.getsize(f)</font>.",
            "code": "files = glob.glob(os.path.join(self.data_dir, '*.parquet'))\nsuspicious = []\nfor f in files:\n    size = os.path.getsize(f)"
        },
        {
            "title": "D. Lógica de Parquets Sospechosos (< 10 KB)",
            "body": "Si un día de mercado entero (Miles de Tickers) es descargado exitosamente de ORATS a través del Paso 1, el archivo resultante "
                    "comprimido siempre pesará múltiples Megabytes. Si un archivo pesa menos de 10 Kilobytes (<font name='Courier' color='#D53F8C'>10 * 1024</font>), "
                    "el script infiere lógicamente que ocurrió un error silencioso: quizás la red se cortó o la API envió un JSON vacío sin arrojar alertas "
                    "en su momento. Anota todos esos nombres de archivo en la lista <i>suspicious</i> para advertirte al final.",
            "code": "    # 10 * 1024 = 10 Kilobytes\n    if os.path.getsize(f) < 10 * 1024: \n        suspicious.append(os.path.basename(f))\n\nif suspicious:\n    print(f'[WARN] Archivos chicos: {suspicious}')"
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
