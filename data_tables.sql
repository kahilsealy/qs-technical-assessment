CREATE TABLE glass_stocks_all_bars(
    glass_type TEXT NOT NULL,
    stock INT NOT NULL,
    bar TEXT NOT NULL,
    FOREIGN KEY(bar) REFERENCES bar_locations(bar),
    FOREIGN KEY(glass_type) REFERENCES glass_types_all_cocktails(glass_type)
);

CREATE TABLE london_daily_transactions (
    date_created DATETIME NOT NULL,
    bar_no INT NOT NULL,
    drink TEXT NOT NULL,
    total_amount FLOAT NOT NULL,
    drinks_sold INT NOT NULL,
    FOREIGN KEY(drink) REFERENCES glass_types_all_cocktails(drink),
    FOREIGN KEY(bar_no) REFERENCES bar_locations(bar_no)
);

CREATE TABLE new_york_daily_transactions (
    date_created DATETIME NOT NULL,
    bar_no INT NOT NULL,
    drink TEXT NOT NULL,
    total_amount FLOAT NOT NULL,
    drinks_sold INT NOT NULL,
    FOREIGN KEY(drink) REFERENCES glass_types_all_cocktails(drink),
    FOREIGN KEY(bar_no) REFERENCES bar_locations(bar_no)
);


CREATE TABLE budapest_daily_transactions (
    date_created DATETIME NOT NULL,
    bar_no INT NOT NULL,
    drink TEXT NOT NULL,
    total_amount FLOAT NOT NULL,
    drinks_sold INT NOT NULL,
    FOREIGN KEY(drink) REFERENCES glass_types_all_cocktails(drink),
    FOREIGN KEY(bar_no) REFERENCES bar_locations(bar_no)
);


CREATE TABLE glass_types_all_cocktails (
    drink TEXT NOT NULL,
    glass_type TEXT NOT NULL,
    FOREIGN KEY(drink) REFERENCES budapest_daily_transactions(drink),
    FOREIGN KEY(drink) REFERENCES london_daily_transactions(drink),
    FOREIGN KEY(drink) REFERENCES new_york_daily_transactions(drink),
    FOREIGN KEY(glass_type) REFERENCES glass_stocks_all_bars(glass_type)
);


CREATE TABLE bar_locations (
    bar_no INT PRIMARY KEY,
    bar TEXT NOT NULL,
    FOREIGN KEY(bar) REFERENCES glass_stocks_all_bars(bar),
    FOREIGN KEY(bar_no) REFERENCES budapest_daily_transactions(bar_no),
    FOREIGN KEY(bar_no) REFERENCES london_daily_transactions(bar_no),
    FOREIGN KEY(bar_no) REFERENCES new_york_daily_transactions(bar_no)
);