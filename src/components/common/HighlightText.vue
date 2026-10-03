<template>
  <template
    v-for="(part, index) in parts"
    :key="index"
  >
    <mark
      v-if="part.matched"
      class="rounded-xs bg-yellow-300 text-black"
      :style="part.style"
    >
      {{ part.text }}
    </mark>
    <span
      v-else-if="part.style"
      :style="part.style"
    >
      {{ part.text }}
    </span>
    <template v-else>{{ part.text }}</template>
  </template>
</template>

<script setup lang="ts">
import { parseAnsiHighlight, type AnsiHighlightSegment } from '@/helper/ansi'
import { getSearchTextParts } from '@/helper/search'
import { themeColorScheme } from '@/helper/theme'
import { computed } from 'vue'

const props = withDefaults(
  defineProps<{
    text: string
    filter: string
    ansi?: boolean
  }>(),
  {
    ansi: false,
  },
)

const parts = computed<AnsiHighlightSegment[]>(() =>
  props.ansi
    ? parseAnsiHighlight(props.text, props.filter, themeColorScheme.value)
    : getSearchTextParts(props.text, props.filter),
)
</script>
