# список товаров (каждый товар — это словарь)
products = [
    {"name": "Laptop", "price": 1200},
    {"name": "Phone", "price": 800},
    {"name": "Tablet", "price": 500},
    {"name": "Monitor", "price": 300},
    {"name": "Headphones", "price": 150}
]

# функция для вывода товара
with open("output.txt", "w") as file:
    for product in products:
        name = product["name"]
        price = product["price"]

        if price > 700:
            line = name + " - " + str(price) + " $ (EXPENSIVE)"
        elif price < 400:
            line = name + " - " + str(price) + " $ (CHEAP)"
        else:
            line = name + " - " + str(price) + " $"

        file.write(line + "\n")

# цикл по всем товарам
