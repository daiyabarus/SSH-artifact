from typing import ClassVar


class ColorPalette:
    color_codes: ClassVar[list[str]] = [
        "#B8860B",  # Dark Goldenrod
        "#9932CC",  # Dark Orchid
        "#E9967A",  # Dark Salmon
        "#8FBC8F",  # Dark Sea Green
        "#8B0000",  # Dark Red
        "#00CED1",  # Dark Turquoise
        "#483D8B",  # Dark Slate Blue
        "#2F4F4F",  # Dark Slate Gray
        "#4B0150",  # Dark Purple
        "#FF8C00",  # Dark Orange
        "#556B2F",  # Dark Olive Green
        "#FF0000",  # Red
        "#00A86B",  # Jade
        "#4B0082",  # Indigo
        "#FFA500",  # Orange
        "#1E90FF",  # Dodger Blue
        "#8B4513",  # Saddle Brown
        "#FF1493",  # Deep Pink
        "#00FF00",  # Lime
        "#800080",  # Purple
        "#FFD700",  # Gold
        "#32CD32",  # Lime Green
        "#FF00FF",  # Magenta
        "#1E4D2B",  # Forest Green
        "#CD853F",  # Peru
        "#00BFFF",  # Deep Sky Blue
        "#DC143C",  # Crimson
        "#7CFC00",  # Lawn Green
        "#FFFF00",  # Yellow
        "#4682B4",  # Steel Blue
        "#800000",  # Maroon
        "#40E0D0",  # Turquoise
        "#FF6347",  # Tomato
        "#6B8E23",  # Olive Drab
        "#FFE4B5",  # Moccasin
        "#008080",  # Teal
        "#FFC0CB",  # Pink
        "#00FF7F",  # Spring Green
        "#FF4500",  # Orange Red
        "#F0E68C",  # Khaki
        "#4169E1",  # Royal Blue
        "#F4A460",  # Sandy Brown
        "#7B68EE",  # Medium Slate Blue
        "#A0522D",  # Sienna
        "#C71585",  # Medium Violet Red
        "#66CDAA",  # Medium Aquamarine
        "#D2691E",  # Chocolate
        "#DB7093",  # Pale Violet Red
        "#DDA0DD",  # Plum
        "#008000",  # Green
    ]

    @classmethod
    def get_color(cls, index: int) -> str:
        return cls.COLORS[index % len(cls.COLORS)]
