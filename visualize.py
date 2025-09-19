
import numpy as np
import matplotlib

def monthly_data_image(monthly, min_val, max_val):
    monthly = np.clip((monthly - min_val) / (max_val - min_val), 0, 1)
    v = monthly.max(0)
    c = monthly.max(0) - monthly.min(0)
    s = c / v
    # s = s / s.max()
    theta = np.arange(12) / 12 * 2 * np.pi
    locations = monthly.transpose(1, 2, 0) @ np.array([np.cos(theta), np.sin(theta)]).T
    h = np.rad2deg(np.atan2(locations[..., 1], locations[..., 0]) % (2 * np.pi)) / 360
    img = (matplotlib.colors.hsv_to_rgb(np.dstack([h, s, v])) * 255).round().astype(np.uint8)
    return img