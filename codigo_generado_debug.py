def generar_plot(df):
    import pandas as pd
    from plotnine import ggplot, aes, geom_line, labs, theme_minimal, theme

    # Preparacion de datos
    df = df[df['MEDIDAS#es'] == 'Sueldos y salarios']
    df['TERRITORIO_CODE'] = df['TERRITORIO_CODE'].astype(str)
    df = df[(df['TERRITORIO_CODE'].str.startswith('ES7')) & (df['TERRITORIO_CODE'].str.len() == 5)]
    df = df[~df['TERRITORIO#es'].isin(['Canarias', 'Las Palmas', 'Santa Cruz de Tenerife'])]
    df['TIME_PERIOD_CODE'] = pd.to_numeric(df['TIME_PERIOD_CODE'], errors='coerce')
    df['OBS_VALUE'] = pd.to_numeric(df['OBS_VALUE'], errors='coerce')
    df = df.dropna(subset=['TIME_PERIOD_CODE', 'OBS_VALUE'])
    df = df.sort_values('TIME_PERIOD_CODE')
    
    # Construccion del grafico
    plot = (ggplot(df, aes(x='TIME_PERIOD_CODE', y='OBS_VALUE', group='TERRITORIO#es', color='TERRITORIO#es')) +
            geom_line(size=0.9, alpha=0.8) +
            labs(title='Evolucion del gasto por isla', subtitle='Sueldos y salarios', x='Ano', y='Gasto en EUR', color='Territorio') +
            theme_minimal() +
            theme(legend_position='bottom'))
    
    return plot
