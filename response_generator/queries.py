import numpy as np

def q1_count(data_by_zone, zone_id, confidence_min=0.0):
    records = data_by_zone.get(zone_id, [])
    return sum(1 for r in records if r["confidence"] >= confidence_min)

def q2_area(data_by_zone, zone_id, confidence_min=0.0):
    records = data_by_zone.get(zone_id, [])
    areas = [r["area_in_meters"] for r in records if r["confidence"] >= confidence_min]

    if not areas:
        return {
            "avg_area": 0.0,
            "total_area": 0.0,
            "n": 0
        }

    return {
        "avg_area": sum(areas) / len(areas),
        "total_area": sum(areas),
        "n": len(areas)
    }

def q3_density(data_by_zone, zone_area_km2, zone_id, confidence_min=0.0):
    count = q1_count(data_by_zone, zone_id, confidence_min)
    area_km2 = zone_area_km2.get(zone_id, 1)
    return count / area_km2

def q4_compare(data_by_zone, zone_area_km2, zone_a, zone_b, confidence_min=0.0):
    density_a = q3_density(data_by_zone, zone_area_km2, zone_a, confidence_min)
    density_b = q3_density(data_by_zone, zone_area_km2, zone_b, confidence_min)

    return {
        "zone_a": zone_a,
        "density_a": density_a,
        "zone_b": zone_b,
        "density_b": density_b,
        "winner": zone_a if density_a > density_b else zone_b
    }

def q5_confidence_dist(data_by_zone, zone_id, bins=5):
    records = data_by_zone.get(zone_id, [])
    scores = [r["confidence"] for r in records]

    if not scores:
        return []

    counts, edges = np.histogram(scores, bins=bins, range=(0, 1))

    result = []
    for i in range(bins):
        result.append({
            "bucket": i,
            "min": float(edges[i]),
            "max": float(edges[i + 1]),
            "count": int(counts[i])
        })

    return result
