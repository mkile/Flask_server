from scipy.spatial import ConvexHull

def convex_hull(points):
    return ConvexHull(points)