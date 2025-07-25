from shared.rollouts import Feature

__all__ = ["Feature"]

# By default, features have one variant:
#    { "enabled": FeatureVariant(True, 1.0) }

READ_NEW_TA = Feature("read_new_ta")
