# Load libraries
library(broom)
library(car)
library(dplyr)
library(FSA)
library(grid)
library(gridExtra)
library(ragg)
library(readxl)
library(tidyr)

# Set working directory
setwd("")

# Load data from Excel file
r_data <- read_excel("R_output200.xlsx")
python_data <- read_excel("Python_output_final200.xlsx")
python_data <- python_data[, 2:ncol(python_data)]

# Add a language column
python_data$language <- "Python"
r_data$language <- "R"

# Combine the data
combined_data <- rbind(python_data, r_data)

# Ensure language is a factor variable
combined_data$language <- as.factor(combined_data$language)

# Convert data to long format for ANOVA
long_data <- combined_data %>%
  pivot_longer(cols = -language, 
               names_to = c("data_type", "operation"), 
               names_sep = "_query_", 
               values_to = "proc_time")

# Perform the ANOVA
anova_result <- aov(proc_time ~ language * operation, data = long_data)

# Display the ANOVA table
summary(anova_result)

# Check for normality (underlying assumption of doing ANOVA)
par(mfrow = c(1, 2))
hist(anova_result$residuals)    # Histogram of residuals
qqPlot(anova_result$residuals,  # QQ plot of residuals
       id = FALSE)
par(mfrow = c(1, 1))

# The histogram and QQ plot of the model residuals are not normal, which violates
# an underlying assumption of conducting ANOVA tests. Due to this, nonparametric 
# tests, the Mann-Whitney U test and the Kruskal-Wallis test will be used to 
# analyze the data instead.

# Non-parametric tests: Mann-Whitney U test for language, Kruskal-Wallis test
# for operation and data_type

# Mann-Whitney U test for language
mann_whitney_language <- wilcox.test(proc_time ~ language, data = long_data)
print(mann_whitney_language)

# Kruskal-Wallis test for data_type
kruskal_data_type <- kruskal.test(proc_time ~ data_type, data = long_data)
print(kruskal_data_type) # p < 0.05

# Post-hoc tests: Dunn test for operation and data_type

# Dunn test for operation
dunn_operation <- dunnTest(proc_time ~ operation,
                          data = long_data,
                          method = "holm")
print(dunn_operation)

# Dunn test for data_type
dunn_data_type <- dunnTest(proc_time ~ data_type,
                          data = long_data,
                          method = "holm")
print(dunn_data_type)

# Save output of statistical tests for use in research paper

# Generate and Save Mann-Whitney U Test Table
mann_whitney_result <- data.frame(
  test = "Mann-Whitney U Test",
  W = mann_whitney_language$statistic,
  p.value = mann_whitney_language$p.value
)
mann_whitney_table <- tableGrob(mann_whitney_result, rows = NULL)
agg_png("mann_whitney_results.png", width = 8, height = 6, units = "in", res = 300)
grid.draw(mann_whitney_table)
dev.off()

# Generate and Save Kruskal-Wallis Test Table for Operations
kruskal_operation_result <- data.frame(
  test = "Kruskal-Wallis Test for Operation",
  chi.squared = kruskal_operation$statistic,
  df = kruskal_operation$parameter,
  p.value = kruskal_operation$p.value
)
kruskal_operation_table <- tableGrob(kruskal_operation_result, rows = NULL)
agg_png("kruskal_operation_results.png", width = 8, height = 6, units = "in", res = 300)
grid.draw(kruskal_operation_table)
dev.off()

# Generate and Save Kruskal-Wallis Test Table for Data Types
kruskal_data_type_result <- data.frame(
  test = "Kruskal-Wallis Test for Data Type",
  chi.squared = kruskal_data_type$statistic,
  df = kruskal_data_type$parameter,
  p.value = kruskal_data_type$p.value
)
kruskal_data_type_table <- tableGrob(kruskal_data_type_result, rows = NULL)
agg_png("kruskal_data_type_results.png", width = 8, height = 6, units = "in", res = 300)
grid.draw(kruskal_data_type_table)
dev.off()

# Generate and Save Dunn Test Table for Operations
dunn_operation_results <- dunn_operation$res %>% as.data.frame()
dunn_operation_table <- tableGrob(dunn_operation_results, rows = NULL)
agg_png("dunn_operation_results.png", width = 8, height = 6, units = "in", res = 300)
grid.draw(dunn_operation_table)
dev.off()

# Generate and Save Dunn Test Table for Data Types
dunn_data_type_results <- dunn_data_type$res %>% as.data.frame()
dunn_data_type_table <- tableGrob(dunn_data_type_results, rows = NULL)
agg_png("dunn_data_type_results.png", width = 8, height = 6, units = "in", res = 300)
grid.draw(dunn_data_type_table)
dev.off()