import pandas as pd
import numpy as np
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression
from scipy.optimize import minimize
import os
import logging
from typing import Tuple, Optional


class StrokeCalculator:
    def __init__(self, csv_path: str = "stroke_pressure_log.csv", degree: int = 4):
        """
        Initialize the stroke calculator with polynomial regression model.

        Args:
            csv_path: Path to the CSV file containing stroke-pressure data
            degree: Polynomial degree for regression (default: 5)
        """
        self.csv_path = csv_path
        self.degree = degree
        self.model = None
        self.poly = None
        self.is_trained = False

        # Set up logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def load_and_train_model(self) -> bool:
        """
        Load data from CSV and train the polynomial regression model.

        Returns:
            bool: True if training successful, False otherwise
        """
        try:
            # Check if CSV file exists
            if not os.path.exists(self.csv_path):
                self.logger.error(f"CSV file not found: {self.csv_path}")
                return False

            # Load data
            df = pd.read_csv(self.csv_path)

            # Validate required columns
            required_columns = ["stroke_1", "stroke_2", "avg_pressure"]
            if not all(col in df.columns for col in required_columns):
                self.logger.error(
                    f"Missing required columns. Expected: {required_columns}"
                )
                return False

            for col in required_columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

            before_rows = len(df)
            df = df.dropna(subset=required_columns)
            dropped = before_rows - len(df)
            if dropped > 0:
                self.logger.warning(
                    f"Dropped {dropped} rows with NaN in required columns"
                )

            if len(df) == 0:
                self.logger.error("No valid training data after dropping NaNs")
                return False

            # Prepare features and target
            X = df[["stroke_1", "stroke_2"]].values
            y = df["avg_pressure"].values

            # Check for sufficient data
            if len(X) < 10:
                self.logger.warning(
                    "Insufficient data for reliable training (< 10 samples)"
                )

            # Create polynomial features
            self.poly = PolynomialFeatures(degree=self.degree)
            X_poly = self.poly.fit_transform(X)

            # Train model
            self.model = LinearRegression()
            self.model.fit(X_poly, y)

            self.is_trained = True
            self.logger.info(f"Model trained successfully with {len(X)} samples")
            return True

        except Exception as e:
            self.logger.error(f"Error training model: {str(e)}")
            return False

    def predict_pressure(self, stroke1: float, stroke2: float) -> Optional[float]:
        """
        Predict pressure from stroke values.

        Args:
            stroke1: First stroke value
            stroke2: Second stroke value

        Returns:
            Predicted pressure or None if model not trained
        """
        if not self.is_trained:
            self.logger.error("Model not trained. Call load_and_train_model() first.")
            return None

        try:
            X_in = np.array([[stroke1, stroke2]])
            X_in_poly = self.poly.transform(X_in)
            return float(self.model.predict(X_in_poly)[0])
        except Exception as e:
            self.logger.error(f"Error predicting pressure: {str(e)}")
            return None

    def get_required_strokes(
        self, required_pressure: float, current_stroke1: float, current_stroke2: float
    ) -> Tuple[Optional[float], Optional[float]]:
        """
        Calculate required stroke values to achieve target pressure.

        Args:
            required_pressure: Target pressure value
            current_stroke1: Current stroke 1 value (used as initial guess)
            current_stroke2: Current stroke 2 value (used as initial guess)

        Returns:
            Tuple of (stroke1, stroke2) or (None, None) if calculation fails
        """
        if not self.is_trained:
            self.logger.error("Model not trained. Call load_and_train_model() first.")
            return None, None

        try:
            # Load data to get bounds
            df = pd.read_csv(self.csv_path)
            s1_min, s1_max = df["stroke_1"].min(), df["stroke_1"].max()
            s2_min, s2_max = df["stroke_2"].min(), df["stroke_2"].max()

            # Objective function: minimize squared error
            def objective(vars):
                s1, s2 = vars
                pred = self.predict_pressure(s1, s2)
                if pred is None:
                    return float("inf")
                return (pred - required_pressure) ** 2

            # Initial guess
            initial_guess = [current_stroke1, current_stroke2]

            # Bounds
            bounds = [(s1_min, s1_max), (s2_min, s2_max)]

            # Optimize
            result = minimize(
                objective, initial_guess, bounds=bounds, method="L-BFGS-B"
            )

            if result.success:
                return float(result.x[0]), float(result.x[1])
            else:
                self.logger.error(f"Optimization failed: {result.message}")
                return None, None

        except Exception as e:
            self.logger.error(f"Error calculating required strokes: {str(e)}")
            return None, None

    def append_training_data(
        self, stroke1: float, stroke2: float, pressure: float
    ) -> bool:
        """
        Append new data point to CSV and retrain model.

        Args:
            stroke1: Stroke 1 value
            stroke2: Stroke 2 value
            pressure: Pressure value

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Create new data point
            new_data = pd.DataFrame(
                {
                    "stroke_1": [stroke1],
                    "stroke_2": [stroke2],
                    "avg_pressure": [pressure],
                }
            )

            # Append to CSV
            if os.path.exists(self.csv_path):
                new_data.to_csv(self.csv_path, mode="a", header=False, index=False)
            else:
                new_data.to_csv(self.csv_path, mode="w", header=True, index=False)

            # Retrain model
            return self.load_and_train_model()

        except Exception as e:
            self.logger.error(f"Error appending training data: {str(e)}")
            return False


# Global instance for reuse
_stroke_calculator = None


def get_stroke_calculator() -> StrokeCalculator:
    """Get global stroke calculator instance."""
    global _stroke_calculator
    if _stroke_calculator is None:
        _stroke_calculator = StrokeCalculator()
    return _stroke_calculator


def load_model() -> bool:
    """Load and train the stroke calculation model."""
    calculator = get_stroke_calculator()
    return calculator.load_and_train_model()


def get_required_strokes(
    required_pressure: float, current_stroke1: float, current_stroke2: float
) -> Tuple[Optional[float], Optional[float]]:
    """Calculate required strokes for target pressure."""
    calculator = get_stroke_calculator()
    return calculator.get_required_strokes(
        required_pressure, current_stroke1, current_stroke2
    )


def append_to_csv(stroke1: float, stroke2: float, pressure: float) -> bool:
    """Append new training data."""
    calculator = get_stroke_calculator()
    return calculator.append_training_data(stroke1, stroke2, pressure)
