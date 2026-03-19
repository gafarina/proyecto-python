import os
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.platypus.flowables import KeepTogether

def create_pdf(filename="explicacion_orats_paso7.pdf"):
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
    story.append(Paragraph("Explicación del Paso 7: Filtro Mínimo de Strikes", title_style))
    story.append(Spacer(1, 0.2 * inch))

    intro_text = (
        "El <b>Paso 7</b> actúa como un controlador de factibilidad matemática para nuestra futura interpolación de curvas de volatilidad y griegas. "
        "Si el Paso 6 ya nos había entregado las opciones más líquidas de la historia, el Paso 7 asegura que cada acción, en cada día de vencimiento puntual, "
        "tenga una cantidad mínima estricta de <b><i>strikes</i></b> (puntos en la curva) para que nuestros algoritmos no colapsen por falta de datos."
    )
    story.append(Paragraph(intro_text, intro_style))

    # 4. Datos Estructurados de la Explicación
    steps_data = [
        {
            "title": "A. El Desafío Matemático (Interpolación)",
            "body": "Para armar la superficie de volatilidad (<i>volatility smile</i>) se aplican matemáticas complejas (como curvas SSVI o Interpolaciones Spline). "
                    "Si después del filtro de liquidez del Paso 6 nos quedan únicamente 1 o 2 contratos sobrevivientes para la fecha de expiración $TSLA en 15 días, "
                    "los modelos fracasarían porque matemáticamente dos puntos trazan solo una recta, no una curva representativa de la volatilidad extrema. "
                    "El filtro requiere un mínimo de 4 strikes.",
            "code": "# Parámetro de entrada clave en run_pipeline.py\ncall_builder.filter_min_strikes(min_strikes=4)\nput_builder.filter_min_strikes(min_strikes=4)"
        },
        {
            "title": "B. Agrupación Multidimensional (DTE)",
            "body": "Usando la velocidad de Polars, el algoritmo agrupa todo el archivo del día cruzando la dimensión de símbolo (<b>ticker</b>) con "
                    "su correspondiente plazo de vencimiento o vida útil de los contratos en días (<b>dte</b> - Days To Expiration o dtex).",
            "code": "df = pl.read_parquet(path)\n\n# Obtiene el nombre real de la columna DTE, que\n# puede variar por cambios en las APIs a través del historial\ndte_col = 'dte' if 'dte' in df.columns else 'dtex'"
        },
        {
            "title": "C. Eliminación Quirúrgica Vertical",
            "body": "Por cada segmento o bloque de agrupación [Ticker + Expiración], el código invoca un filtro analítico. "
                    "Evalúa el conteo total de filas (strikes). Todo el bloque completo cuya cantidad sea menor al nivel <font name='Courier' color='#D53F8C'>min_strikes</font> "
                    "será removido radicalmente del archivo en memoria para evitar enviar datos parcializados a los cerebros del sistema de pricing posteriores.",
            "code": "df_filtered = (\n    df.group_by(['ticker', dte_col])\n    .filter(pl.count('strike') >= min_strikes)\n)"
        },
        {
            "title": "D. Sobreescritura Eficiente en Disco",
            "body": "Un pilar del pipeline es jamás sobreescribir el disco por las puras si el DataFrame ha sido idéntico (es decir, ningún bloque falló y todo superó la prueba). "
                    "El resultado solo reemplazará con alta velocidad el archivo original si las cantidades de filas difieren, usando <font name='Courier' color='#D53F8C'>os.rename</font> temporal para asegurar inmunidad frente a apagones durante el guardado.",
            "code": "if df_filtered.height < df.height:\n    tmp_path = path + '.tmp'\n    df_filtered.write_parquet(tmp_path, compression='snappy')\n    os.remove(path)\n    os.rename(tmp_path, path)"
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
