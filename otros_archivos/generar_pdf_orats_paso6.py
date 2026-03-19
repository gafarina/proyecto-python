import os
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.platypus.flowables import KeepTogether

def create_pdf(filename="explicacion_orats_paso6.pdf"):
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
    story.append(Paragraph("Explicación del Paso 6: Generación de Ruedas Filtradas", title_style))
    story.append(Spacer(1, 0.2 * inch))

    intro_text = (
        "El <b>Paso 6</b> es el primer gran filtro de 'limpieza' algorítmica. La base de datos cruda de la API ORATS contiene una inmensa cantidad "
        "de 'basura' financiera (opciones ilíquidas, spreads prohibitivos, quotes absurdos o strikes tan profundos <i>In the Money</i> que su precio está distorsionado). "
        "A través de las clases <font name='Courier' color='#D53F8C'>WheelsCallEarnBuilder</font> y <font name='Courier' color='#D53F8C'>WheelsPutEarnBuilder</font>, "
        "separaremos el trigo de la paja extrayendo exclusivamente el material útil para modelar volatilidad (OTM/ATM)."
    )
    story.append(Paragraph(intro_text, intro_style))

    # 4. Datos Estructurados de la Explicación
    steps_data = [
        {
            "title": "A. Recorte de Universo (El Filtro de Pertenencia)",
            "body": "No nos interesa procesar los miles de activos diarios. El builder lee el <font name='Courier' color='#D53F8C'>universo.json</font> "
                    "que generamos previamente (nuestra lista dorada selectiva) y, al leer cada archivo parquet diario usando <b>Polars</b>, elimina instantáneamente todos "
                    "los símbolos descartados de la memoria.",
            "code": "df = pl.scan_parquet(path)\ndf = df.filter(pl.col('ticker').is_in(self.universo_tickers))"
        },
        {
            "title": "B. Exigencia de Existencia (Bid, Ask, Volumen & Open Interest)",
            "body": "Los creadores de mercado muchas veces dejan 'precios fantasma'. Un filtro indispensable es exigir que existan compradores reales, "
                    "vendedores reales y que la opción haya estado viva ese día. Se elimina cualquier dato con <font name='Courier' color='#D53F8C'>Bid=0</font> o <font name='Courier' color='#D53F8C'>Ask=0</font>. "
                    "Asimismo, se requiere <font name='Courier' color='#D53F8C'>Volume > 0</font> y <font name='Courier' color='#D53F8C'>OpenInterest > 0</font>.",
            "code": "condition = (\n    (pl.col(cBid) > 0) &\n    (pl.col(cAsk) > 0) &\n    (pl.col(cVol) > 0) &\n    (pl.col(cOi) > 0)\n)"
        },
        {
            "title": "C. Filtro Direccional (ATM y OTM Exclusivo)",
            "body": "Las opciones sumergidas <i>In The Money</i> (ITM) están dominadas por el valor intrínseco, lo cual oscurece su componente de volatilidad. "
                    "Para las <b>Calls</b>, exigimos <font name='Courier' color='#D53F8C'>Strike >= stockPrice</font>. Para las <b>Puts</b>, por simetría, se exige "
                    "<font name='Courier' color='#D53F8C'>Strike <= stockPrice</font>. Esto aísla el valor extrínseco puro.",
            "code": "# Para Calls:\ncondition = condition & (pl.col('strike') >= pl.col('stockPrice'))\n\n# Para Puts:\ncondition = condition & (pl.col('strike') <= pl.col('stockPrice'))"
        },
        {
            "title": "D. El Filtro del Spread (< 40%)",
            "body": "Incluso si existe Bid y Ask, un Ask exorbitantemente alto vs un Bid microscópico (Bid $0.10, Ask $10.00) es un dato inútil. "
                    "Se aplica un filtro implacable sobre la diferencia Bid/Ask obligando a que el radio <font name='Courier' color='#D53F8C'>Ask / Bid</font> sea menor "
                    "a 1.40 (un 40% de separación máxima tolerada).",
            "code": "condition = condition & ((pl.col(cAsk) / pl.col(cBid)) <= 1.40)"
        },
        {
            "title": "E. Cálculo del Mid Price y Bifurcación de Directorios",
            "body": "Con las opciones filtradas, agregamos la columna pilar del pricing: el <font name='Courier' color='#D53F8C'>MidPrice</font>, "
                    "calculado automáticamente como <font name='Courier' color='#D53F8C'>(Ask + Bid) / 2</font>. Al final, estos DataFrames destilados "
                    "se guardan limpiamente en subcarpetas separadas: <font name='Courier' color='#D53F8C'>ruedas_call_earn</font> y <font name='Courier' color='#D53F8C'>ruedas_put_earn</font>.",
            "code": "df_res = df.with_columns([\n    ((pl.col(cBid) + pl.col(cAsk)) / 2).alias(cMid)\n])\n\nout_p = os.path.join(self.output_dir, f'call_{date_str}.parquet')\ndf_res.write_parquet(out_p, compression='snappy')"
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
