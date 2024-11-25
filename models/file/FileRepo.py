import math
from fastapi import HTTPException
import pandas as pd



class FileRepo():
    def __init__(self) -> None:
        pass

    def readFile(self, path: str):
        # Inclui a coluna DATARM diretamente
        columns_to_read = ["DATARM", "NOMECLI", "TOTAL_SEM_IPI", "TOTAL_IPI", "DESCONTO", "CODMERCRM", 'UFCLI', 'CIDCLI', "CODREPRM", "NOME_MERCA"]
        df = pd.read_excel(path, usecols=columns_to_read, engine='openpyxl', nrows=9000)
        
        # Certifique-se de que DATARM está no formato datetime
        df['DATARM'] = pd.to_datetime(df['DATARM'], format='%m/%d/%Y')  # Ajuste o formato conforme necessário
        return df

    def analyzeSales(self, dataframe: pd.DataFrame, granularity: str, year: str):
        """
        Analisa as vendas, descontos e outros dados agrupados por período.

        Parameters:
        - dataframe (pd.DataFrame): DataFrame contendo os dados de vendas.
        - granularity (str): Nível de granularidade ("daily", "monthly", "yearly").
        - year (str): Ano para filtrar os dados. Pode ser um ano específico ("2023") ou "all" para incluir todos os anos.

        Returns:
        - dict: Dados agrupados contendo vendas totais, descontos, vendas por cliente, representante, localização e produto.
        """
        # Certifique-se de que a coluna de data está no formato datetime
        dataframe['DATARM'] = pd.to_datetime(dataframe['DATARM'], format='%m/%d/%Y')

        # Filtrar por ano, se necessário
        if year != "all":
            dataframe = dataframe[dataframe['DATARM'].dt.year == int(year)]

        # Escolha a granularidade
        if granularity == "daily":
            dataframe['Interval'] = dataframe['DATARM'].dt.date
        elif granularity == "monthly":
            dataframe['Interval'] = dataframe['DATARM'].dt.to_period('M').astype(str)
        elif granularity == "yearly":
            dataframe['Interval'] = dataframe['DATARM'].dt.year
        else:
            raise ValueError("Granularity must be one of: 'daily', 'monthly', 'yearly'.")

        # Dados principais: Vendas e Descontos
        sales_data = dataframe.groupby('Interval').agg(
            Total_Sales=('TOTAL_IPI', 'sum'),
            Total_Sales_No_IPI=('TOTAL_SEM_IPI', 'sum'),
            Total_Discounts_With_IPI=('TOTAL_IPI', 'sum'),
            Total_Discounts_Discount=('DESCONTO', 'sum')
        ).reset_index()

        # Vendas por cliente
        sales_by_client = dataframe.groupby(['Interval', 'NOMECLI']).agg(
            Total_Sales_No_IPI=('TOTAL_SEM_IPI', 'sum'),
            Total_Discounts_With_IPI=('TOTAL_IPI', 'sum'),
            Total_Discounts_Discount=('DESCONTO', 'sum')
        ).reset_index()

        # Vendas por representante
        sales_by_rep = dataframe.groupby(['Interval', 'CODREPRM']).agg(
            Total_Sales_No_IPI=('TOTAL_SEM_IPI', 'sum'),
            Total_Discounts_With_IPI=('TOTAL_IPI', 'sum'),
            Total_Discounts_Discount=('DESCONTO', 'sum')
        ).reset_index()

        # Distribuição geográfica
        cid_sales = dataframe.groupby(['Interval', 'CIDCLI']).agg(
            Total_Sales_No_IPI=('TOTAL_SEM_IPI', 'sum'),
            Total_Discounts_With_IPI=('TOTAL_IPI', 'sum'),
            Total_Discounts_Discount=('DESCONTO', 'sum')
        ).reset_index()
        uf_sales = dataframe.groupby(['Interval', 'UFCLI']).agg(
            Total_Sales_No_IPI=('TOTAL_SEM_IPI', 'sum'),
            Total_Discounts_With_IPI=('TOTAL_IPI', 'sum'),
            Total_Discounts_Discount=('DESCONTO', 'sum'),
            Frequency=('UFCLI', 'count')
        ).reset_index()

        # Produtos mais vendidos
        top_products = dataframe.groupby(['Interval', 'NOME_MERCA', 'CODMERCRM']).agg(
            Total_Sales_No_IPI=('TOTAL_SEM_IPI', 'sum'),
            Total_Discounts_With_IPI=('TOTAL_IPI', 'sum'),
            Total_Discounts_Discount=('DESCONTO', 'sum'),
            Frequency=('CODMERCRM', 'count')
        ).reset_index()

        # Converta todos os agrupamentos para lista de dicionários
        result = {
            "Total_Sales_and_Discounts": sales_data.to_dict(orient="records"),
            "Sales_by_Client": sales_by_client.to_dict(orient="records"),
            "Sales_by_Agent": sales_by_rep.to_dict(orient="records"),
            "Cid_Sales": cid_sales.to_dict(orient="records"),
            "UF_Sales": uf_sales.to_dict(orient="records"),
            "Top_Products": top_products.to_dict(orient="records")
        }

        return result

        
