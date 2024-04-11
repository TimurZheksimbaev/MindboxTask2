from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def get_product_category_pairs(products_df, categories_df, product_category_df):
    # Объединяем продукты и категории по связям
    product_category_pairs = product_category_df.join(products_df, product_category_df.product_id == products_df.product_id) \
        .join(categories_df, product_category_df.category_id == categories_df.category_id) \
        .select(products_df.product_name, categories_df.category_name)

    # Находим продукты, у которых нет категорий
    # left anit join - возвращает столбцы из левого датафрейма которые не были найдены в правом
    # lit - создает столбец с названием переданным в alias() и с значением переданным в lit()
    products_without_categories = products_df.join(product_category_df, products_df.product_id == product_category_df.product_id, 'left_anti') \
        .select(products_df.product_name, lit('No category').alias('category_name'))

    # Объединяем результаты
    result = product_category_pairs.union(products_without_categories)

    return result

def main():
    # Инициализация SparkSession
    spark = SparkSession.builder.appName("app").getOrCreate()

    # Создаем тестовые датафреймы
    # датафреймы с продуктмаи, категориями и связями
    products_df = spark.createDataFrame([(1, 'Product 1'), (2, 'Product 2'), (3, 'Product 3'), (4, 'Product 4')],
                                        ['product_id', 'product_name'])
    categories_df = spark.createDataFrame([(1, 'Category 1'), (2, 'Category 2')], ['category_id', 'category_name'])
    product_category_df = spark.createDataFrame([(1, 1), (1, 2), (2, 2), (3, 1)], ['product_id', 'category_id'])

    result = get_product_category_pairs(products_df, categories_df, product_category_df)
    result.show()

if __name__ == "__main__":
    main()